import sys, time, threading
import minimalmodbus
from datetime import datetime
from pathlib import Path
import pandas as pd
import csv
import serial
import struct

import requests

from PySide6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QFileDialog, QTableWidget, QTableWidgetItem, QHeaderView,
    QSizePolicy, QSpacerItem, QFrame, QMessageBox
)
from PySide6.QtCore import QTimer, Qt
from PySide6.QtGui import QColor

from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure

# ================= CONFIG (Môi trường) ==================
PORT = "/dev/ttyUSB0"
BAUD = 9600
ID_TEMP_HUM = 1
ID_WIND_SPD = 3
ID_WIND_DIR = 4
READ_INTERVAL_MS = 1000
CSV_AUTO_DIR = Path.cwd()
MAX_SAMPLES = 200
TABLE_HEADERS = ["Time", "Temperature (°C)", "Humidity (%)", "Wind Direction (°)", "Wind Speed (m/s)"]

# ================= CONFIG (ADXL qua Serial/USB) ==================
ADXL_PORT = "/dev/ttyUSB1"      # Cổng USB cắm mạch ADXL
ADXL_BAUD = 115200
PACKET_SIZE = 28
NODES = {b'\xA5': 1, b'\xA6': 2}
ADXL_HEADERS = ["pc_time", "node_id", "esp32_micros", "z_value"]

# ================= REALTIME SERVER CONFIG ==================
SERVER_URL = "http://100.110.169.51:8080"
API_KEY    = "iotserver"
DEVICE_ID  = "raspi-01"

ADXL_BATCH_SIZE = 50
ADXL_FLUSH_INTERVAL_S = 0.15


# ================= REALTIME SENDER ==================
class RealtimeSender(threading.Thread):
    def __init__(self, server_url: str, api_key: str, device_id: str,
                 timeout: float = 2.0, adxl_batch_size: int = 50, adxl_flush_interval_s: float = 0.15):
        super().__init__(daemon=True)
        self.server_url = server_url.rstrip("/")
        self.api_key = api_key
        self.device_id = device_id
        self.timeout = timeout

        self.adxl_batch_size = int(adxl_batch_size)
        self.adxl_flush_interval_s = float(adxl_flush_interval_s)

        self._running = True
        self._lock = threading.Lock()

        self._rs485_buf = []
        self._adxl_buf = []
        self._adxl_last_flush = time.time()

        self._sess = requests.Session()
        self._headers = {"X-API-Key": self.api_key}

    def stop(self):
        self._running = False

    def push_rs485(self, sample: dict):
        with self._lock:
            self._rs485_buf.append(sample)

    def push_adxl_sample(self, z1: int, z2: int, z3: int):
        with self._lock:
            self._adxl_buf.append([int(z1), int(z2), int(z3)])

    def _post(self, body: dict):
        self._sess.post(
            f"{self.server_url}/ingest",
            json=body,
            headers=self._headers,
            timeout=self.timeout
        )

    def run(self):
        while self._running:
            # 1) gửi RS485
            rs_item = None
            with self._lock:
                if self._rs485_buf:
                    rs_item = self._rs485_buf.pop(0)

            if rs_item is not None:
                body = {
                    "device_id": self.device_id,
                    "ts": datetime.utcnow().isoformat() + "Z",
                    "type": "rs485",
                    "sample": rs_item
                }
                try:
                    self._post(body)
                except Exception:
                    pass

            # 2) flush ADXL batch
            now = time.time()
            chunk = None
            with self._lock:
                need_flush = (len(self._adxl_buf) >= self.adxl_batch_size) or \
                             ((now - self._adxl_last_flush) >= self.adxl_flush_interval_s and len(self._adxl_buf) > 0)
                if need_flush:
                    take = min(self.adxl_batch_size, len(self._adxl_buf))
                    chunk = self._adxl_buf[:take]
                    del self._adxl_buf[:take]
                    self._adxl_last_flush = now

            if chunk is not None:
                body = {
                    "device_id": self.device_id,
                    "ts": datetime.utcnow().isoformat() + "Z",
                    "type": "adxl_batch",
                    "fs_hz": 500,
                    "chunk_start_us": int(chunk[0][0]),
                    "samples": chunk
                }
                try:
                    self._post(body)
                except Exception:
                    pass

            time.sleep(0.001)


# ================= HELPER ==================
def make_instrument(addr: int):
    inst = minimalmodbus.Instrument(PORT, addr)
    inst.serial.baudrate = BAUD
    inst.serial.bytesize = 8
    inst.serial.parity = minimalmodbus.serial.PARITY_NONE
    inst.serial.stopbits = 1
    inst.serial.timeout = 1.0
    inst.mode = minimalmodbus.MODE_RTU
    inst.clear_buffers_before_each_transaction = True
    inst.close_port_after_each_call = True
    return inst

def deg_to_cardinal(deg: float) -> str:
    try:
        d = float(deg) % 360.0
    except Exception:
        return "-"
    dirs = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
    idx = int((d + 22.5) // 45) % 8
    return dirs[idx]

def calculate_checksum(data):
    cs = 0
    for b in data:
        cs ^= b
    return cs


# ================= ADXL SERIAL LOGGER THREAD ==================
class ADXLLogger(threading.Thread):
    """
    Đọc 2 ADXL345 qua Polling Serial (COM5) và ghép thành mảng 3 giá trị cho UI/Realtime
    """
    def __init__(self, csv_path: Path, realtime_sender=None):
        super().__init__(daemon=True)
        self.csv_path = csv_path
        self._running = True

        self._lock = threading.Lock()
        
        # Cache giữ giá trị mới nhất của các trục
        self.latest_z1 = 0
        self.latest_z2 = 0
        self.latest_z3 = 0  # Dummy, luôn = 0 để giữ nguyên giao diện UI
        self._latest_tuple = (0, 0, 0)

        self.realtime_sender = realtime_sender

    def stop(self):
        self._running = False

    def get_latest(self):
        with self._lock:
            return self._latest_tuple

    def run(self):
        # Mở cổng Serial
        try:
            ser = serial.Serial(ADXL_PORT, ADXL_BAUD, timeout=0)
            ser.reset_input_buffer()
        except Exception as e:
            print(f"Lỗi không mở được cổng {ADXL_PORT}: {e}")
            return # Thoát luồng nếu không có cổng COM

        # Chuẩn bị file CSV
        self.csv_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            csv_file = open(self.csv_path, "w", newline="")
            writer = csv.writer(csv_file)
            writer.writerow(ADXL_HEADERS)
        except Exception as e:
            print(f"Lỗi mở file CSV ADXL: {e}")
            try: ser.close()
            except: pass
            return

        raw_buffer = bytearray()

        while self._running:
            try:
                for poll_cmd, node_id in NODES.items():
                    if not self._running:
                        break

                    # Gửi lệnh Polling
                    ser.write(poll_cmd)
                    time.sleep(0.015)

                    # Đọc dữ liệu
                    if ser.in_waiting > 0:
                        raw_buffer.extend(ser.read(ser.in_waiting))

                    # Bóc tách gói tin (Packet = 28 bytes)
                    while len(raw_buffer) >= PACKET_SIZE:
                        if raw_buffer[0] == 0xAA:
                            packet = raw_buffer[:PACKET_SIZE]
                            
                            # Kiểm tra byte kết thúc, node id và checksum
                            if packet[27] == 0x55 and packet[1] == node_id:
                                if packet[26] == calculate_checksum(packet[0:26]):
                                    now_dt = datetime.now()
                                    pc_time_str = now_dt.strftime("%H:%M:%S.%f")[:-3]
                                    start_micros = struct.unpack('<I', packet[2:6])[0]
                                    last_z = 0

                                    # Gói tin hợp lệ: bóc 10 mẫu Z
                                    for i in range(10):
                                        z = struct.unpack('<h', packet[6 + i * 2: 8 + i * 2])[0]
                                        sample_micros = start_micros + (i * 5000)
                                        
                                        # Cập nhật Cache an toàn
                                        with self._lock:
                                            if node_id == 1:
                                                self.latest_z1 = z
                                            elif node_id == 2:
                                                self.latest_z2 = z
                                            
                                            self._latest_tuple = (self.latest_z1, self.latest_z2, self.latest_z3)
                                            curr_z1, curr_z2, curr_z3 = self._latest_tuple

                                        # Ghi log file
                                        writer.writerow([pc_time_str, node_id, sample_micros, z])

                                        # Đẩy vào RealtimeSender (tương tự 500Hz cũ)
                                        if self.realtime_sender is not None:
                                            try:
                                                self.realtime_sender.push_adxl_sample(curr_z1, curr_z2, curr_z3)
                                            except Exception:
                                                pass

                                        last_z = z

                                    print(
                                        f"[{pc_time_str}] Node {node_id} | "
                                        f"Micros: {start_micros:10d} | "
                                        f"Z_last: {last_z:5d}"
                                    )

                                    del raw_buffer[:PACKET_SIZE]
                                    continue
                                    
                            del raw_buffer[0] # Header đúng nhưng Checksum/ID/EndByte sai
                        else:
                            del raw_buffer[0] # Rác
            except Exception as e:
                time.sleep(0.05) # Giảm tải CPU nếu có lỗi đọc ghi
                pass

        # Cleanup khi stop
        try:
            csv_file.close()
            ser.close()
        except:
            pass


# ================= SIMPLE PLOT ==================
class SimplePlot(FigureCanvas):
    def __init__(self, ylabel="", title=""):
        fig = Figure(figsize=(4, 2.5), tight_layout=True)
        super().__init__(fig)
        self.ax = fig.add_subplot(111)
        fig.patch.set_facecolor('#0c0c0c')
        self.ax.set_facecolor('#222')
        self.ax.set_title(title, color='w', fontsize=12, fontweight='bold')
        self.ax.set_ylabel(ylabel, color='w', fontsize=11)
        self.ax.tick_params(colors='w', labelsize=9)
        for spine in self.ax.spines.values():
            spine.set_color('#888')
        self.ax.grid(True, color='#555', linestyle='--', linewidth=0.6)

    def plot_series(self, times, values, title, color=None, y_fixed_range=None):
        self.ax.clear()
        self.ax.set_facecolor('#222')
        self.ax.set_title(title, color='w', fontsize=12, fontweight='bold')
        self.ax.set_ylabel(self.ax.get_ylabel(), color='w', fontsize=11)

        if len(times) > 0 and len(values) > 0:
            x = list(range(len(values)))
            if color:
                self.ax.plot(x, values, marker='o', markersize=4, linewidth=2.0, linestyle='-', color=color)
            else:
                self.ax.plot(x, values, marker='o', markersize=4, linewidth=2.0, linestyle='-')

            step = max(1, len(times) // 8)
            ticks = list(range(0, len(times), step))
            labels = [times[i].strftime("%H:%M") for i in ticks]
            self.ax.set_xticks(ticks)
            self.ax.set_xticklabels(labels, rotation=30, color='w', fontsize=9)

            if y_fixed_range:
                self.ax.set_ylim(y_fixed_range)
                y_min, y_max = y_fixed_range
                if "Temp" in title:
                    self.ax.set_yticks(list(range(y_min, y_max + 1, 5)))
                elif "Humid" in title:
                    self.ax.set_yticks(list(range(y_min, y_max + 1, 10)))
            else:
                y_max = max(values) * 1.2 if max(values) > 0 else 1
                self.ax.set_ylim(0, y_max)

        self.ax.tick_params(colors='w', labelsize=9)
        for spine in self.ax.spines.values():
            spine.set_color('#888')
        self.ax.grid(True, color='#555', linestyle='--', linewidth=0.6)
        self.draw()


# ================= MAIN DASHBOARD ==================
class Dashboard(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Sensor Manager (RS485_ADXL345)")
        self.resize(1450, 860)

        self.inst_temp = make_instrument(ID_TEMP_HUM)
        self.inst_dir  = make_instrument(ID_WIND_DIR)
        self.inst_spd  = make_instrument(ID_WIND_SPD)

        self.data_times = []
        self.data_temp = []
        self.data_hum = []
        self.data_wdir_deg = []
        self.data_wspd = []

        self.adxl_logger = None
        self.adxl_csv_path = None
        self.rt_sender = None

        main = QVBoxLayout(self)
        main.setContentsMargins(12,12,12,12)
        main.setSpacing(10)

        # Topbar buttons
        topbar = QHBoxLayout()
        self.btnExportExcelADXL = QPushButton("Export Excel ADXL345")
        self.btnExportExcelRS485 = QPushButton("Export Excel RS485")
        self.btnStart = QPushButton("Start")
        self.btnStop = QPushButton("Stop"); self.btnStop.setEnabled(False)
        self.btnRefresh = QPushButton("Refresh")
        self.btnExportExcelADXL.clicked.connect(self.export_excel_adxl_dialog)
        self.btnExportExcelRS485.clicked.connect(self.export_excel_rs485_dialog)
        self.btnStart.clicked.connect(self.start_reading)
        self.btnStop.clicked.connect(self.stop_reading)
        self.btnRefresh.clicked.connect(self.redraw_plots)
        topbar.addWidget(self.btnExportExcelADXL)
        topbar.addWidget(self.btnExportExcelRS485)
        topbar.addWidget(self.btnStart)
        topbar.addWidget(self.btnStop)
        topbar.addItem(QSpacerItem(40,20,QSizePolicy.Expanding,QSizePolicy.Minimum))
        topbar.addWidget(self.btnRefresh)
        main.addLayout(topbar)

        # Tiles Layout
        tiles_layout = QHBoxLayout(); tiles_layout.setSpacing(12)
        self.tile_temp = self._tile_unified("Temperature", "°C", "#00cc66")
        self.tile_hum  = self._tile_unified("Humidity", "%", "#4da6ff")
        self.tile_wdir = self._tile_unified("Wind Direction", "", "#ff9a33", with_subline=True)
        self.tile_wspd = self._tile_unified("Wind Speed", "m/s", "#ffcc00")
        
        # ADXL tiles
        self.tile_adxl1 = self._tile_unified("ADXL345 1", "Z", "#c77dff")
        self.tile_adxl2 = self._tile_unified("ADXL345 2", "Z", "#ff4d6d")
        self.tile_adxl3 = self._tile_unified("ADXL345 3", "Z", "#00d4ff")

        tiles_layout.addWidget(self.tile_temp)
        tiles_layout.addWidget(self.tile_hum)
        tiles_layout.addWidget(self.tile_wdir)
        tiles_layout.addWidget(self.tile_wspd)
        tiles_layout.addWidget(self.tile_adxl1)
        tiles_layout.addWidget(self.tile_adxl2)
        tiles_layout.addWidget(self.tile_adxl3)
        tiles_layout.addStretch(1)
        main.addLayout(tiles_layout)

        # Table
        self.table = QTableWidget(0, len(TABLE_HEADERS))
        self.table.setHorizontalHeaderLabels(TABLE_HEADERS)
        header = self.table.horizontalHeader()
        for i in range(len(TABLE_HEADERS)):
            header.setSectionResizeMode(i, QHeaderView.Stretch)
        self.table.verticalHeader().setVisible(False)
        self.table.setAlternatingRowColors(True)
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.table.setSelectionMode(QTableWidget.NoSelection)
        self.table.setFocusPolicy(Qt.NoFocus)
        self.table.setStyleSheet("""
            QTableWidget { background-color: #0f0f0f; alternate-background-color: #1a1a1a;
                gridline-color: #2e2e2e; color: #ffffff; }
            QHeaderView::section { background-color: #1f1f1f; color: #ffffff; font-weight: 700; font-size: 14px; }
        """)
        main.addWidget(self.table, stretch=5)

        # Plots
        plots_rt = QHBoxLayout()
        self.plot_temp = SimplePlot(ylabel="°C", title="Temperature (°C)")
        self.plot_hum  = SimplePlot(ylabel="%", title="Humidity (%)")
        self.plot_wspd = SimplePlot(ylabel="m/s", title="Wind Speed (m/s)")
        plots_rt.addWidget(self.plot_temp,1)
        plots_rt.addWidget(self.plot_hum,1)
        plots_rt.addWidget(self.plot_wspd,1)
        main.addLayout(plots_rt, stretch=3)

        # Timers
        self.timer = QTimer(); self.timer.timeout.connect(self.read_all)
        self.csv_path = None
        self.apply_dark_style()

    def _tile_unified(self, title, unit, color, with_subline=False):
        f = QFrame()
        f.setFrameShape(QFrame.StyledPanel)
        f.setMinimumWidth(200); f.setMinimumHeight(140)
        v = QVBoxLayout(f); v.setContentsMargins(16,14,16,14); v.setSpacing(6)
        lbl_title = QLabel(title)
        lbl_title.setStyleSheet("color:#ffffff; font-weight:600; font-size:13px;")
        v.addWidget(lbl_title)

        lbl_value = QLabel("-")
        lbl_value.setObjectName(f"tile_value_{title.replace(' ','_')}")
        lbl_value.setStyleSheet(f"color:{color}; font-size:36px; font-weight:800;")
        v.addWidget(lbl_value)

        if with_subline:
            lbl_sub = QLabel("")
            lbl_sub.setObjectName(f"tile_sub_{title.replace(' ','_')}")
            lbl_sub.setStyleSheet("color:#bbbbbb; font-size:14px;")
            v.addWidget(lbl_sub)
        else:
            v.addWidget(QLabel(""))

        lbl_unit = QLabel(unit)
        lbl_unit.setStyleSheet("color:#ffffff; font-size:13px;")
        v.addWidget(lbl_unit)

        f.setStyleSheet("QFrame {background:#2a2a2a; border-radius:12px;}")
        return f

    def apply_dark_style(self):
        self.setStyleSheet("""
        QWidget { background:#0b0b0b; color:#ddd; font-family:Arial; }
        QPushButton { background:#1f6feb; color:#fff; padding:6px 10px; border-radius:6px; }
        QPushButton:hover { background:#2a8df0; }
        """)

    def start_reading(self):
        now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        self.csv_path = CSV_AUTO_DIR / f"rs485_log_{now}.csv"
        pd.DataFrame(columns=TABLE_HEADERS).to_csv(self.csv_path, index=False)

        self.adxl_csv_path = CSV_AUTO_DIR / f"adxl345_log_{now}.csv"

        if self.rt_sender is None:
            self.rt_sender = RealtimeSender(
                SERVER_URL, API_KEY, DEVICE_ID,
                timeout=2.0, adxl_batch_size=ADXL_BATCH_SIZE, adxl_flush_interval_s=ADXL_FLUSH_INTERVAL_S
            )
            self.rt_sender.start()

        # Init ADXLLogger (Bây giờ chạy qua Serial)
        self.adxl_logger = ADXLLogger(self.adxl_csv_path, realtime_sender=self.rt_sender)
        self.adxl_logger.start()

        self.timer.start(READ_INTERVAL_MS)
        self.btnStart.setEnabled(False); self.btnStop.setEnabled(True)

    def stop_reading(self):
        self.timer.stop()
        self.btnStart.setEnabled(True); self.btnStop.setEnabled(False)

        if self.adxl_logger is not None:
            try: self.adxl_logger.stop()
            except Exception: pass
            self.adxl_logger = None

        if self.rt_sender is not None:
            try: self.rt_sender.stop()
            except Exception: pass
            self.rt_sender = None

    def export_excel_rs485_dialog(self):
        fname, _ = QFileDialog.getSaveFileName(
            self, "Save Excel",
            f"sensor_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            "Excel Files (*.xlsx)"
        )
        if not fname: return
        rows = []
        for r in range(self.table.rowCount()):
            row = []
            for c in range(self.table.columnCount()):
                item = self.table.item(r, c)
                row.append(item.text() if item else "")
            rows.append(row)
        df = pd.DataFrame(rows, columns=TABLE_HEADERS)
        df.to_excel(fname, index=False)
        QMessageBox.information(self, "Export", f"Exported to {fname}")

    def export_excel_adxl_dialog(self):
        if not self.adxl_csv_path or not Path(self.adxl_csv_path).exists():
            QMessageBox.warning(self, "Export", "Chưa có file adxl345_log để export (hãy bấm Start trước).")
            return
        fname, _ = QFileDialog.getSaveFileName(
            self, "Save Excel",
            f"adxl345_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            "Excel Files (*.xlsx)"
        )
        if not fname: return
        try:
            df = pd.read_csv(self.adxl_csv_path, on_bad_lines="skip")
            df.to_excel(fname, index=False)
            QMessageBox.information(self, "Export", f"Exported to {fname}")
        except Exception as ex:
            QMessageBox.warning(self, "Export", f"Không export được: {ex}")

    def read_all(self):
        t = datetime.now()
        try:
            self.inst_temp.address = ID_TEMP_HUM
            raw_temp = self.inst_temp.read_register(0, functioncode=3)
            time.sleep(0.05)
            raw_hum  = self.inst_temp.read_register(1, functioncode=3)
            time.sleep(0.05)

            self.inst_spd.address = ID_WIND_SPD
            raw_wspd = self.inst_spd.read_register(0, functioncode=3)
            time.sleep(0.05)

            self.inst_dir.address = ID_WIND_DIR
            raw_wdir = self.inst_dir.read_register(0, functioncode=3)
            time.sleep(0.05)
        except Exception:
            raw_temp = raw_hum = raw_wdir = raw_wspd = None

        temp = (raw_temp / 10.0) if raw_temp is not None else None
        hum  = (raw_hum  / 10.0) if raw_hum  is not None else None
        wspd = (raw_wspd / 10.0) if raw_wspd is not None else None

        wdir_deg = None if raw_wdir is None else float(raw_wdir) % 360.0
        wdir_txt = "-" if wdir_deg is None else deg_to_cardinal(wdir_deg)

        self.findChild(QLabel, "tile_value_Temperature").setText("" if temp is None else f"{temp:.1f}")
        self.findChild(QLabel, "tile_value_Humidity").setText("" if hum  is None else f"{hum:.1f}")
        self.findChild(QLabel, "tile_value_Wind_Speed").setText("" if wspd is None else f"{wspd:.1f}")
        self.findChild(QLabel, "tile_value_Wind_Direction").setText(wdir_txt)

        sub = self.findChild(QLabel, "tile_sub_Wind_Direction")
        if sub is not None:
            sub.setText("" if wdir_deg is None else f"{int(wdir_deg)}°")

        # Cập nhật ADXL (Lấy từ Tuple Cache)
        latest = None
        if self.adxl_logger is not None:
            try: latest = self.adxl_logger.get_latest()
            except Exception: latest = None

        lbl1 = self.findChild(QLabel, "tile_value_ADXL345_1")
        lbl2 = self.findChild(QLabel, "tile_value_ADXL345_2")
        lbl3 = self.findChild(QLabel, "tile_value_ADXL345_3") # Sẽ luôn in số 0
        if latest is None:
            if lbl1: lbl1.setText("-")
            if lbl2: lbl2.setText("-")
            if lbl3: lbl3.setText("-")
        else:
            z1, z2, z3 = latest
            if lbl1: lbl1.setText(f"{z1}")
            if lbl2: lbl2.setText(f"{z2}")
            if lbl3: lbl3.setText(f"{z3}")

        # Table update
        if self.table.rowCount() >= 600:
            self.table.removeRow(0)

        r = self.table.rowCount()
        self.table.insertRow(r)

        time_str = t.strftime("%Y-%m-%d %H:%M:%S")
        items = [
            QTableWidgetItem(time_str),
            QTableWidgetItem("" if temp is None else f"{temp:.1f}"),
            QTableWidgetItem("" if hum  is None else f"{hum:.1f}"),
            QTableWidgetItem("" if wdir_deg is None else f"{int(wdir_deg)}"),
            QTableWidgetItem("" if wspd is None else f"{wspd:.1f}")
        ]
        colors = ["#FFFFFF", "#00cc66", "#4da6ff", "#ff9a33", "#ffcc00"]

        for i, it in enumerate(items):
            it.setTextAlignment(Qt.AlignCenter)
            it.setForeground(QColor(colors[i]))
            f = it.font(); f.setBold(True); it.setFont(f)
            it.setFlags(Qt.ItemIsEnabled)
            self.table.setItem(r, i, it)

        self.table.scrollToBottom()

        # CSV append RS485
        if self.csv_path:
            pd.DataFrame([[
                time_str,
                ("" if temp is None else f"{temp:.1f}"),
                ("" if hum  is None else f"{hum:.1f}"),
                ("" if wdir_deg is None else f"{int(wdir_deg)}"),
                ("" if wspd is None else f"{wspd:.1f}")
            ]], columns=TABLE_HEADERS).to_csv(
                self.csv_path, mode='a', header=False, index=False
            )

        if self.rt_sender is not None:
            try:
                self.rt_sender.push_rs485({
                    "time_local": time_str,
                    "temp_c": temp,
                    "hum_pct": hum,
                    "wind_dir_deg": wdir_deg,
                    "wind_dir_txt": wdir_txt,
                    "wind_spd_ms": wspd,
                })
            except Exception:
                pass

        self.data_times.append(t)
        self.data_temp.append(temp)
        self.data_hum.append(hum)
        self.data_wdir_deg.append(wdir_deg)
        self.data_wspd.append(wspd)

        if len(self.data_times) > MAX_SAMPLES:
            self.data_times = self.data_times[-MAX_SAMPLES:]
            self.data_temp = self.data_temp[-MAX_SAMPLES:]
            self.data_hum = self.data_hum[-MAX_SAMPLES:]
            self.data_wdir_deg = self.data_wdir_deg[-MAX_SAMPLES:]
            self.data_wspd = self.data_wspd[-MAX_SAMPLES:]

        self.redraw_plots()

    def redraw_plots(self):
        def filtered(t, v):
            t2, v2 = [], []
            for tt, vv in zip(t, v):
                if vv is None: continue
                t2.append(tt); v2.append(vv)
            return t2, v2

        t1, v1 = filtered(self.data_times, self.data_temp)
        t2, v2 = filtered(self.data_times, self.data_hum)
        t4, v4 = filtered(self.data_times, self.data_wspd)

        self.plot_temp.plot_series(t1, v1, "Temperature (°C)", "#00cc66", y_fixed_range=(10, 50))
        self.plot_hum.plot_series(t2, v2, "Humidity (%)", "#4da6ff", y_fixed_range=(0, 100))
        self.plot_wspd.plot_series(t4, v4, "Wind Speed (m/s)", "#ffcc00")

    def closeEvent(self, e):
        if self.adxl_logger is not None:
            try: self.adxl_logger.stop()
            except Exception: pass
            self.adxl_logger = None

        if self.rt_sender is not None:
            try: self.rt_sender.stop()
            except Exception: pass
            self.rt_sender = None

        e.accept()

# ================= RUN ==================
if __name__ == "__main__":
    app = QApplication(sys.argv)
    w = Dashboard()
    w.show()
    sys.exit(app.exec())
