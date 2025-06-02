import customtkinter as ctk
import tkinter as tk # Lisää tämä tiedoston alkuun, jos ei jo ole
from tkinter import ttk
from tkinter import filedialog, messagebox, simpledialog # scrolledtext will be replaced by CTkTextbox
import subprocess
import threading
import time
import serial
import serial.tools.list_ports
import sys
import os
import queue
import json
from typing import List, Dict, Optional, Tuple, Any
import copy # Needed for deep copies
import uuid # For unique test step IDs
import traceback # For detailed error logging
import csv # LISÄTTY CSV-IMPORT

# --- Nidaqmx import and check ---
try:
    import nidaqmx
    import numpy as np
    from nidaqmx.constants import LineGrouping, TerminalConfiguration, AcquisitionType, Edge, Slope, DigitalDriveType
    NIDAQMX_AVAILABLE = True
except ImportError:
    NIDAQMX_AVAILABLE = False
except OSError as e:
    NIDAQMX_AVAILABLE = False

# --- Pymodbus import and check ---
try:
    from pymodbus.client.serial import ModbusSerialClient
    from pymodbus.exceptions import ModbusIOException, ConnectionException, ModbusException
    from pymodbus.pdu import ExceptionResponse
    PYMODBUS_AVAILABLE = True
except ImportError:
    PYMODBUS_AVAILABLE = False
    # print("WARNING: pymodbus library not found. Modbus testing unavailable.") # Poistettu turha printtaus

# --- Configuration ---
MAX_DEVICES = 10
FLASH_ADDR_BOOTLOADER = "0x1000"
FLASH_ADDR_PARTITIONS = "0x8000"
FLASH_ADDR_APP        = "0x10000"
DEFAULT_BAUD_RATE_FLASH = "921600"
DEFAULT_BAUD_RATE_SERIAL = "115200"
SERIAL_READ_TIMEOUT = 1.0
DAQ_DEVICE_NAME = "Dev1"
DEFAULT_MODBUS_SLAVE_ID = 1
DEFAULT_MODBUS_BAUDRATE = 9600
DEFAULT_MODBUS_TIMEOUT = 1.0
DAQ_SAMPLE_RATE = 1000
DAQ_NUM_SAMPLES_PER_CHANNEL = 100
DAQ_DIO_SETTLE_TIME_S = 0.05

RESULT_CIRCLE_SIZE = 100
RESULT_CIRCLE_PADDING = 20
RESULT_TEXT_OFFSET_X = RESULT_CIRCLE_SIZE / 2
RESULT_TEXT_OFFSET_Y = RESULT_CIRCLE_SIZE / 2
RESULT_PASS_COLOR = "#4CAF50"
RESULT_FAIL_COLOR = "#F44336"
RESULT_RUNNING_COLOR = "#FFC107"
RESULT_NONE_COLOR = "#E0E0E0"
RESULT_SKIP_COLOR = "#9E9E9E"

AVAILABLE_TEST_TYPES = {
    'flash': 'Ohjelmointi',
    'serial': 'Sarjatesti',
    'daq': 'DAQ',
    'modbus': 'Modbus',
    'wait_info': 'Odotus/Info',
    'daq_and_serial': 'DAQ ja Sarjatesti (Rinnakkain)', # UUSI
    'daq_and_modbus': 'DAQ ja Modbus (Rinnakkain)',    # UUSI
    'optical_inspection': 'Optinen Tarkastus'
}

ctk.set_appearance_mode("Dark")
ctk.set_default_color_theme("dark-blue")


def list_serial_ports() -> List[str]:
    ports = serial.tools.list_ports.comports()
    return sorted([port.device for port in ports])

def build_esptool_args(port: str, baud: str, files: Dict[str, str]) -> Optional[List[str]]:
    if not port: return None
    required_keys = ['bootloader', 'partitions', 'app']
    if not all(key in files and files[key] for key in required_keys): return None
    if not os.path.exists(files['bootloader']): print(f"WARNING: Bootloader file not found: {files['bootloader']}")
    if not os.path.exists(files['partitions']): print(f"WARNING: Partitions file not found: {files['partitions']}")
    if not os.path.exists(files['app']): print(f"WARNING: App file not found: {files['app']}")
    args = ["--chip", "esp32", "--port", port, "--baud", baud, "--before", "default_reset",
            "--after", "hard_reset", "write_flash", "-z", "--flash_mode", "dio",
            "--flash_freq", "40m", "--flash_size", "detect",
            FLASH_ADDR_BOOTLOADER, files['bootloader'],
            FLASH_ADDR_PARTITIONS, files['partitions'],
            FLASH_ADDR_APP, files['app']]
    return args

def run_composite_test_worker(device_index: int,
                              composite_type: str, # 'daq_and_serial' tai 'daq_and_modbus'
                              daq_settings: Dict,
                              serial_settings: Optional[Dict], # Vain jos daq_and_serial
                              modbus_settings: Optional[Dict], # Vain jos daq_and_modbus
                              app_config: Dict, # Sis. portit, baudit jne.
                              stop_event: threading.Event,
                              app_gui_queue: queue.Queue,
                              daq_lock_ref: threading.Lock, # Viite pääsovelluksen DAQ-lukkoon
                              get_daq_in_use_by_device_func, # Funktio nykyisen DAQ-käyttäjän saamiseksi
                              set_daq_in_use_by_device_func, # Funktio DAQ-käyttäjän asettamiseksi
                              add_to_daq_wait_queue_func,    # Funktio DAQ-jonoon lisäämiseksi
                              check_daq_queue_func_from_app # Funktio DAQ-jonon tarkistamiseksi pääsovelluksesta
                              ):

    # Apufunktio GUI-jonoon kirjoittamiseen
    def q(type_str: str, data: Any = None, color: str = 'black', sub_test:str = ""):
        log_prefix = f"[{sub_test.upper()}] " if sub_test else ""
        app_gui_queue.put({'type': type_str, 'device_index': device_index, 'data': f"{log_prefix}{data}", 'status_color': color})

    # Apufunktio aliworkerin tuloksen käsittelyyn
    sub_test_results = {}
    sub_test_threads = []
    sub_stop_events = {'daq': threading.Event(), 'serial': threading.Event(), 'modbus': threading.Event()}

    overall_success = True # Oletetaan onnistuminen

    # --- DAQ-osion käynnistys (aina mukana komposiitissa) ---
    q('output', '[KOMPOSIT] Yritetään DAQ-lukkoa...', sub_test='daq')
    daq_thread = None
    daq_started_successfully = False
    daq_resource_acquired = False

    # GUI-päivitys
    q('status', f"{composite_type.replace('_', ' ').capitalize()} käynnistyy...", 'orange')
    q('clear_output')
    q('output', f"--- Aloitetaan yhdistelmätesti: {composite_type} ---")

    try:
        # 1. Yritä hankkia DAQ-lukko
        if not daq_lock_ref.acquire(blocking=False):
            holder_dev_idx = get_daq_in_use_by_device_func()
            holder = f"Laite {holder_dev_idx}" if holder_dev_idx is not None else "Tuntematon"
            q('output', f"[DAQ] DAQ varattu ({holder}), lisätään jonoon...", color='orange')
            current_daq_user = get_daq_in_use_by_device_func()
            if current_daq_user is not None and current_daq_user != device_index:
                 q('output', f"[DAQ] DAQ varattu laitteella {current_daq_user}. Odotetaan...", color='orange')
                 q('output', "[KOMPOSIT] DAQ varattu, yhdistelmätestiä ei voi aloittaa nyt.", color='red')
                 app_gui_queue.put({'type': 'composite_test_done', 'device_index': device_index, 'data': False})
                 return

            if daq_lock_ref.acquire(blocking=True, timeout=1.0):
                set_daq_in_use_by_device_func(device_index)
                daq_resource_acquired = True
                q('output', "[DAQ] DAQ-lukko saatu.")
                app_gui_queue.put({'type': 'busy_flag_update', 'device_index': device_index, 'flag_name': 'daq', 'value': True})


                def daq_worker_wrapper():
                    try:
                        run_daq_test_worker(device_index, DAQ_DEVICE_NAME, daq_settings, sub_stop_events['daq'], app_gui_queue)
                        sub_test_results['daq'] = True
                    except Exception as e_daq_wrap:
                        q('output', f"[DAQ] Wrapper virhe: {e_daq_wrap}", color='red', sub_test='daq')
                        sub_test_results['daq'] = False
                    finally:
                        if daq_resource_acquired and get_daq_in_use_by_device_func() == device_index:
                            app_gui_queue.put({'type': 'release_daq_lock', 'device_index': device_index, 'data': None})

                daq_thread = threading.Thread(target=daq_worker_wrapper, daemon=True)
                                q('output', '[KOMPOSIT] Käynnistetään DAQ-alisäie...', sub_test='daq')
daq_thread.start()
                sub_test_threads.append(daq_thread)
                daq_started_successfully = True
            else:
                q('output', "[DAQ] DAQ-lukon saanti epäonnistui timeoutilla.", color='red')

                q('output', '[KOMPOSIT] DAQ-alitestiä ei suoriteta (lukkovirhe).', color='red', sub_test='daq')
                app_gui_queue.put({'type': 'composite_test_done', 'device_index': device_index, 'data': False})
                return

        other_thread = None
        other_test_type = ""

        if composite_type == 'daq_and_serial' and serial_settings:
            other_test_type = "serial"
            def serial_worker_wrapper():
                try:
                    run_serial_test_worker(device_index, app_config['monitor_port'], int(app_config['serial_baudrate']), serial_settings, sub_stop_events['serial'], app_gui_queue)
                    sub_test_results['serial'] = True
                except Exception as e_serial_wrap:
                    q('output', f"[SERIAL] Wrapper virhe: {e_serial_wrap}", color='red', sub_test='serial')
                    sub_test_results['serial'] = False
                finally:
                    app_gui_queue.put({'type': 'busy_flag_update', 'device_index': device_index, 'flag_name': 'monitor_port', 'value': False})

            app_gui_queue.put({'type': 'busy_flag_update', 'device_index': device_index, 'flag_name': 'monitor_port', 'value': True})
            other_thread = threading.Thread(target=serial_worker_wrapper, daemon=True)

        elif composite_type == 'daq_and_modbus' and modbus_settings:
            other_test_type = "modbus"
            def modbus_worker_wrapper():
                try:
                    run_modbus_test_worker(device_index, app_config['modbus_port'], int(app_config['modbus_slave_id']), modbus_settings, int(app_config['modbus_baudrate']), float(app_config['modbus_timeout']), sub_stop_events['modbus'], app_gui_queue)
                    sub_test_results['modbus'] = True
                except Exception as e_modbus_wrap:
                    q('output', f"[MODBUS] Wrapper virhe: {e_modbus_wrap}", color='red', sub_test='modbus')
                    sub_test_results['modbus'] = False
                finally:
                    app_gui_queue.put({'type': 'busy_flag_update', 'device_index': device_index, 'flag_name': 'modbus_port', 'value': False})

            app_gui_queue.put({'type': 'busy_flag_update', 'device_index': device_index, 'flag_name': 'modbus_port', 'value': True})
            other_thread = threading.Thread(target=modbus_worker_wrapper, daemon=True)

        if other_thread:
            other_thread.start()
            sub_test_threads.append(other_thread)

        while any(t.is_alive() for t in sub_test_threads):
            if stop_event.is_set():
                q('output', f"Yhdistelmätesti {composite_type} keskeytetään...", color='orange')
                if daq_thread and daq_thread.is_alive(): sub_stop_events['daq'].set()
                if other_thread and other_thread.is_alive(): sub_stop_events[other_test_type].set()
                overall_success = False
                break
            time.sleep(0.1)

        for t in sub_test_threads:
            t.join(timeout=2.0)
            if t.is_alive():
                q('output', f"Ali-säie {t.name} ei pysähtynyt ajoissa!", color='red')
                overall_success = False

                        # Log DAQ thread status specifically
                if daq_thread:
                    if daq_thread.is_alive():
                        q('output', '[KOMPOSIT] DAQ-alisäie ei päättynyt join-timeoutin aikana.', color='red', sub_test='daq')
                    else:
                        q('output', '[KOMPOSIT] DAQ-alisäie päättynyt.', sub_test='daq')
                else:
                    q('output', '[KOMPOSIT] DAQ-alisäiettä ei käynnistetty.', sub_test='daq')

                # Log result from sub_test_results for DAQ
                daq_sub_result_str = str(sub_test_results.get('daq', 'Ei tulosta'))
                q('output', f"[KOMPOSIT] DAQ-alitestin tulos (sub_test_results): {daq_sub_result_str}", sub_test='daq')
if not stop_event.is_set():
            if daq_started_successfully and not sub_test_results.get('daq', False):
                q('output', "[DAQ] DAQ-alitesti epäonnistui.", color='red')
                overall_success = False
            if other_thread and not sub_test_results.get(other_test_type, False):
                q('output', f"[{other_test_type.upper()}] {other_test_type}-alitesti epäonnistui.", color='red')
                overall_success = False

            if overall_success:
                 q('output', f"Yhdistelmätesti {composite_type} suoritettu: OK", color='green')
            else:
                 q('output', f"Yhdistelmätesti {composite_type} suoritettu: VIRHE", color='red')
        else:
            q('output', f"Yhdistelmätesti {composite_type} keskeytetty.", color='orange')
            overall_success = False

    except Exception as e:
        q('output', f"Kriittinen virhe yhdistelmätestissä ({composite_type}): {e}", color='red')
        q('output', traceback.format_exc(), color='red')
        overall_success = False
    finally:
        for sev in sub_stop_events.values(): sev.set()

        if daq_resource_acquired and get_daq_in_use_by_device_func() == device_index:
            app_gui_queue.put({'type': 'release_daq_lock', 'device_index': device_index, 'data': None})

        if not overall_success and not stop_event.is_set():
            q('status', f"{composite_type.replace('_',' ')} Virhe", 'red')
        elif stop_event.is_set():
            q('status', f"{composite_type.replace('_',' ')} Keskeyt.", 'orange')
        else:
            q('status', f"{composite_type.replace('_',' ')} OK", 'green')

        app_gui_queue.put({'type': f'{composite_type}_done', 'device_index': device_index, 'data': overall_success})

def run_flash_worker(device_index: int, command_args: List[str], stop_event: threading.Event, app_gui_queue: queue.Queue):
    thread_id = threading.get_ident()
    flash_success = False
    process = None
    def q(type_str: str, data: Any=None, color: str='black'):
         app_gui_queue.put({'type': type_str, 'device_index': device_index, 'data': data, 'thread_id': thread_id, 'status_color': color})
    try:
        q('status', "Flashataan...", 'orange'); q('clear_output')
        full_command = [sys.executable, "-m", "esptool"] + command_args
        q('output', f"Käynnistetään Flash: {' '.join(full_command)}\n" + '-' * 40 + '\n')
        creationflags = subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0
        process = subprocess.Popen(full_command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1, universal_newlines=True, creationflags=creationflags)
        while True:
            if stop_event.is_set():
                q('output', "\n--- Flashaus keskeytetty ---")
                try:
                    if process.poll() is None: process.terminate(); process.wait(timeout=2)
                except Exception as term_err: print(f"Error terminating/killing process: {term_err}")
                flash_success = False; break
            line = process.stdout.readline()
            if line: q('output', line.strip())
            elif process.poll() is not None: break
            else: time.sleep(0.005)
        if process.stdout: process.stdout.close()
        rc = -1
        if not stop_event.is_set(): rc = process.wait()
        if not stop_event.is_set() and rc == 0: flash_success = True; q('status', 'Flash OK', 'green')
        elif not stop_event.is_set(): flash_success = False; q('status', f'Flash Virhe ({rc})', 'red'); q('output', f"\n--- Flash Virhe! Koodi {rc} ---")
        else: flash_success = False; q('status', 'Flash Keskeyt.', 'orange')
    except FileNotFoundError: q('status', 'Flash Virhe', 'red'); q('output', '\n--- Virhe: esptool ei löydy. ---'); flash_success = False
    except Exception as e: q('status', 'Flash Virhe', 'red'); q('output', f'\n--- Flash Virhe: {e} ---'); flash_success = False
    finally:
        q('flash_done', flash_success)

def run_serial_test_worker(device_index: int, port: str, baudrate: int, test_settings: Dict, stop_event: threading.Event, app_gui_queue: queue.Queue):
    success = False
    def q(type_str: str, data: Any=None, color: str='black'): app_gui_queue.put({'type':type_str,'device_index':device_index,'data':data,'status_color':color})
    q('status',"Sarjatesti...",'orange'); q('clear_output'); q('output', f"Aloitetaan Sarjatesti: Portti={port}, Baud={baudrate}\n")
    duration = test_settings.get("duration_s", 3.0)
    keyword = test_settings.get("keyword", "")
    error_strings = test_settings.get("error_strings", [])
    start_time = time.time()
    found_keyword = not keyword
    found_error = False
    try:
        with serial.Serial(port, baudrate, timeout=SERIAL_READ_TIMEOUT) as ser:
            q('output', f"Sarjaportti {port} avattu {baudrate} bps.")
            command_to_send = test_settings.get("command", "")
            if command_to_send: ser.write(command_to_send.encode() + b'\n'); q('output', f"Lähetetty: {command_to_send}")

            while time.time() - start_time < duration:
                if stop_event.is_set():
                    q('output', "\n--- Sarjatesti keskeytetty ---")
                    success = False
                    break
                try:
                    line = ser.readline().decode(errors='ignore').strip()
                    if line:
                        q('output', line)

                        if keyword and not found_keyword:
                            is_match = (keyword in line) if test_settings.get("case_sensitive", False) else (keyword.lower() in line.lower())
                            if is_match:
                                found_keyword = True
                                q('output', f"Avainsana '{keyword}' LÖYTYI: {line}")
                                q('output', "Avainsana löytyi, lopetetaan sarjamonitorin lukeminen.")
                                break

                        for err_s in error_strings:
                            is_err_match = (err_s in line) if test_settings.get("case_sensitive", False) else (err_s.lower() in line.lower())
                            if is_err_match:
                                q('output', f"VIRHE '{err_s}' LÖYTYI: {line}")
                                found_error = True
                                break
                        if found_error:
                            break

                    else:
                        time.sleep(0.005)

                except serial.SerialException as se:
                    q('output', f"Sarjaporttivirhe: {se}")
                    found_error = True
                    break
            else:
                if not stop_event.is_set():
                    q('output', "\n--- Sarjatestin maksimikesto saavutettu ---")

        if found_error:
            success = False
            q('output', "\n--- Virhe löydetty sarjatestissä tai porttivirhe ---")
        elif test_settings.get("require_keyword", False) and not found_keyword:
            success = False
            q('output', "\n--- Avainsanaa ei löydetty (vaadittu), vaikka maksimikesto saavutettiin tai lukeminen lopetettiin ---")
        else:
            success = True

        if stop_event.is_set():
            success = False

        if success:
            q('status', 'Serial OK', 'green')
        elif stop_event.is_set():
            q('status', 'Serial Keskeyt.', 'orange')
        else:
            q('status', 'Serial Virhe', 'red')

    except serial.SerialException as e:
        q('status', 'Serial Portti Virhe', 'red')
        q('output', f'\n--- Sarjaporttivirhe (avaus epäonnistui): {e} ---')
        success = False
    except Exception as e:
        q('status', 'Serial Yleisvirhe', 'red')
        q('output', f'\n--- Sarjatestin odottamaton virhe: {e} ---')
        traceback.print_exc()
        success = False
    finally:
        q('serial_test_done', success)

def run_daq_test_worker(device_index: int, daq_device_name: str, settings: Dict, stop_event: threading.Event, app_gui_queue: queue.Queue):
    test_success = True
    task_ao, task_ai, task_do, task_di = None, None, None, None

    def q(type_str: str, data: Any = None, color: str = 'black'):
        app_gui_queue.put({'type': type_str, 'device_index': device_index, 'data': data, 'status_color': color})

    def log_q(message: str, error: bool = False):
        prefix = "ERROR: " if error else ""
        q('daq_log', f"DAQ D{device_index}: {prefix}{message}")

    def format_dio_line_for_nidaqmx(daq_dev_name: str, line_name_from_config: str) -> Optional[str]:
        parts = line_name_from_config.split('.')
        if len(parts) == 2 and parts[0].startswith('P'):
            try:
                port_num_str = parts[0][1:]
                line_num_str = parts[1]
                int(port_num_str); int(line_num_str)
                return f"{daq_dev_name}/port{port_num_str}/line{line_num_str}"
            except ValueError:
                log_q(f"Virheellinen DIO-linjan muoto konfiguraatiossa: {line_name_from_config}", error=True)
                return None
        log_q(f"Tuntematon DIO-linjan muoto konfiguraatiossa: {line_name_from_config}", error=True)
        return None

    try:
        q('status', "DAQ Testi...", 'orange')
        log_q(f"--- Aloitetaan DAQ-testi laitteella: {daq_device_name} (Lukko oletetusti hallussa) ---")

        ao_channels_to_use = {ch: cfg for ch, cfg in settings.get("ao_channels", {}).items() if cfg.get("use")}
        if ao_channels_to_use:
            log_q(f"Asetetaan analogialähdöt: {list(ao_channels_to_use.keys())}")
            task_ao = nidaqmx.Task(f"AO_Task_D{device_index}")
            ao_data_to_write = []
            for ch_name, ch_cfg in ao_channels_to_use.items():
                physical_channel = f"{daq_device_name}/{ch_name}"
                task_ao.ao_channels.add_ao_voltage_chan(physical_channel, name_to_assign_to_channel=ch_name, min_val=-10.0, max_val=10.0)
                log_q(f"  Lisätty AO-kanava: {physical_channel} nimellä {ch_name}")
                ao_data_to_write.append(float(ch_cfg.get("output_v", 0.0)))
            if ao_data_to_write:
                 log_q(f"  Kirjoitetaan AO-arvot: {ao_data_to_write}")
                 task_ao.write(ao_data_to_write, auto_start=True)
            log_q("Analogialähdöt asetettu.")
        if stop_event.is_set(): raise Exception("Testi keskeytetty AO-asetuksen jälkeen")

        dio_settings = settings.get("dio_lines", {})
        do_lines_by_nidaqmx_port: Dict[str, List[Tuple[str, bool]]] = {}
        for line_name_config, line_cfg in dio_settings.items():
            if line_cfg.get("use") and line_cfg.get("direction") == "Output":
                nidaqmx_line_name = format_dio_line_for_nidaqmx(daq_device_name, line_name_config)
                if not nidaqmx_line_name: test_success = False; continue
                parts = nidaqmx_line_name.split('/'); nidaqmx_port_channel_name = f"{parts[0]}/{parts[1]}"
                is_high = line_cfg.get("output_val", "Low").lower() == "high"
                if nidaqmx_port_channel_name not in do_lines_by_nidaqmx_port: do_lines_by_nidaqmx_port[nidaqmx_port_channel_name] = []
                do_lines_by_nidaqmx_port[nidaqmx_port_channel_name].append( (nidaqmx_line_name, is_high) )
        if not test_success: raise Exception("Virheellisiä DIO-linjamäärityksiä DO:lle.")
        if do_lines_by_nidaqmx_port:
            log_q("Asetetaan digitaaliulostulot...")
            task_do = nidaqmx.Task(f"DO_Task_D{device_index}")
            unique_ports_added_to_task = set(); all_do_task_channel_names = []
            for nidaqmx_port_channel_name_from_settings in do_lines_by_nidaqmx_port.keys():
                 if nidaqmx_port_channel_name_from_settings not in unique_ports_added_to_task:
                    try:
                        task_channel_do_name = nidaqmx_port_channel_name_from_settings.split('/')[-1]
                        task_do.do_channels.add_do_chan(
                            nidaqmx_port_channel_name_from_settings,
                            name_to_assign_to_lines=task_channel_do_name,
                            line_grouping=LineGrouping.CHAN_FOR_ALL_LINES
                        )
                        log_q(f"  Lisätty DO-portti taskiin: {nidaqmx_port_channel_name_from_settings} nimellä {task_channel_do_name}")
                        unique_ports_added_to_task.add(nidaqmx_port_channel_name_from_settings)
                        all_do_task_channel_names.append(task_channel_do_name)
                    except Exception as e_do_add:
                        log_q(f"Virhe DO-portin lisäyksessä {nidaqmx_port_channel_name_from_settings}: {e_do_add}", error=True); test_success=False; break
            if not test_success: raise Exception("DO-portin lisäys epäonnistui.")
            if task_do.do_channels:
                data_to_write_per_port_channel = []
                for task_channel_name in all_do_task_channel_names:
                    original_port_channel_name_for_settings = f"{daq_device_name}/{task_channel_name}"
                    port_value_uint32 = np.uint32(0)
                    if original_port_channel_name_for_settings in do_lines_by_nidaqmx_port:
                        for nidaqmx_line_name, is_high_state in do_lines_by_nidaqmx_port[original_port_channel_name_for_settings]:
                            try:
                                line_index = int(nidaqmx_line_name.split('/')[-1].replace("line",""))
                                if is_high_state: port_value_uint32 |= (1 << line_index)
                            except (IndexError, ValueError): log_q(f"Virhe parsittaessa linjaindeksiä DO:lle: {nidaqmx_line_name}",error=True)
                        data_to_write_per_port_channel.append(port_value_uint32)
                    else: data_to_write_per_port_channel.append(np.uint32(0))
                if data_to_write_per_port_channel:
                    do_data_array = np.array(data_to_write_per_port_channel, dtype=np.uint32)
                    log_q(f"Kirjoitetaan DO-data: {do_data_array}"); task_do.write(do_data_array, auto_start=True); time.sleep(DAQ_DIO_SETTLE_TIME_S); log_q("Digitaaliulostulot asetettu.")
        if stop_event.is_set(): raise Exception("Testi keskeytetty DO-asetuksen jälkeen")

        di_lines_to_read_config: Dict[str, Dict] = {}
        for line_name_config, line_cfg_from_settings in dio_settings.items():
            if line_cfg_from_settings.get("use") and line_cfg_from_settings.get("direction") == "Input":
                nidaqmx_line_name = format_dio_line_for_nidaqmx(daq_device_name, line_name_config)
                if not nidaqmx_line_name: test_success = False; continue
                di_lines_to_read_config[nidaqmx_line_name] = line_cfg_from_settings
        if not test_success: raise Exception("Virheellisiä DIO-linjamäärityksiä DI:lle.")
        if di_lines_to_read_config:
            log_q("Luetaan digitaalitulot...")
            task_di = nidaqmx.Task(f"DI_Task_D{device_index}")
            di_channels_in_task_order = []
            for nidaqmx_line_name, line_cfg_from_settings in di_lines_to_read_config.items():
                try:
                    task_channel_di_name = nidaqmx_line_name.replace("/", "_")
                    task_di.di_channels.add_di_chan(nidaqmx_line_name, name_to_assign_to_lines=task_channel_di_name,line_grouping=LineGrouping.CHAN_PER_LINE)
                    log_q(f"  Lisätty DI-kanava: {nidaqmx_line_name} (Task-nimi: {task_channel_di_name})")
                    di_channels_in_task_order.append(nidaqmx_line_name)
                except Exception as e: log_q(f"Virhe DI-kanavan lisäyksessä {nidaqmx_line_name}: {e}",error=True);test_success=False;break
            if not test_success: raise Exception("DI-kanavan lisäys epäonnistui.")
            if task_di.di_channels:
                read_di_values_list = task_di.read(number_of_samples_per_channel=1)

                if not isinstance(read_di_values_list, list) or len(read_di_values_list) != len(di_channels_in_task_order):
                    log_q(f"Odottamaton muoto tai pituus DI-datalle. Saatu: {read_di_values_list}, Odotettu pituus: {len(di_channels_in_task_order)}", error=True)
                    test_success = False
                else:
                    for i, phys_name in enumerate(di_channels_in_task_order):
                        cfg_for_current_di = di_lines_to_read_config[phys_name]
                        actual_bool = read_di_values_list[i]
                        exp_str = cfg_for_current_di.get("expected_input","Ignore")
                        u_name = cfg_for_current_di.get("name",phys_name)
                        actual_str="High" if actual_bool else "Low"
                        log_q(f"  Linja {u_name} ({phys_name}): Luettu={actual_str}, Odotettu={exp_str}")
                        if exp_str.lower()!="ignore" and actual_str.lower()!=exp_str.lower():log_q("    VIRHE: Ei täsmää!",error=True);test_success=False
                        elif exp_str.lower()!="ignore":log_q("    OK.")
            if task_di: task_di.close(); task_di = None
        if stop_event.is_set(): raise Exception("Testi keskeytetty DI-lukemisen jälkeen")

        ai_channels_to_read: Dict[str, Dict] = {f"{daq_device_name}/{ch}": cfg for ch,cfg in settings.get("ai_channels",{}).items() if cfg.get("use")}
        if ai_channels_to_read:
            log_q("Luetaan analogiatulot...")
            task_ai = nidaqmx.Task(f"AI_Task_D{device_index}")
            ai_physical_names_in_task_order = []
            channel_counter_for_name = 0
            for ch_physical_name, ch_cfg_from_settings in ai_channels_to_read.items():
                min_val,max_val = float(ch_cfg_from_settings.get("min_v",-10.0)),float(ch_cfg_from_settings.get("max_v",10.0))
                task_channel_name = f"ai_task_chan_{channel_counter_for_name}"; channel_counter_for_name+=1
                try:
                    task_ai.ai_channels.add_ai_voltage_chan(ch_physical_name,name_to_assign_to_channel=task_channel_name,terminal_config=TerminalConfiguration.DEFAULT,min_val=min_val,max_val=max_val)
                    log_q(f"  Lisätty AI-kanava: {ch_physical_name} (Task-nimi: {task_channel_name}, Rajat: {min_val:.2f}V - {max_val:.2f}V)")
                    ai_physical_names_in_task_order.append(ch_physical_name)
                except Exception as e: log_q(f"Virhe AI-kanavan lisäyksessä {ch_physical_name}: {e}",error=True);test_success=False;break
            if not test_success: raise Exception("AI-kanavan lisäys epäonnistui.")
            if task_ai.ai_channels:
                actual_num_samples = DAQ_NUM_SAMPLES_PER_CHANNEL if DAQ_NUM_SAMPLES_PER_CHANNEL > 0 else 1
                task_ai.timing.cfg_samp_clk_timing(rate=DAQ_SAMPLE_RATE,sample_mode=AcquisitionType.FINITE,samps_per_chan=actual_num_samples)
                log_q(f"  Näytteenotto: {DAQ_SAMPLE_RATE} Hz, {actual_num_samples} näytettä/kanava.")
                read_ai_data = task_ai.read(number_of_samples_per_channel=actual_num_samples, timeout=10.0)

                if isinstance(read_ai_data, list): read_ai_data = np.array(read_ai_data)
                if read_ai_data.ndim == 1 and len(ai_physical_names_in_task_order) == 1: read_ai_data = read_ai_data.reshape((1, -1))
                elif read_ai_data.ndim == 0 and len(ai_physical_names_in_task_order) == 1 and actual_num_samples == 1: read_ai_data = np.array([[read_ai_data]])

                expected_shape = (len(ai_physical_names_in_task_order), actual_num_samples)
                if not isinstance(read_ai_data,np.ndarray) or read_ai_data.shape != expected_shape:
                    log_q(f"Odottamaton muoto tai koko AI-datalle. Saatu shape: {read_ai_data.shape if isinstance(read_ai_data, np.ndarray) else type(read_ai_data)}, Odotettu: {expected_shape}", error=True);test_success=False
                else:
                    log_q(f"  Luettu data (shape): {read_ai_data.shape}")
                    for i,phys_name in enumerate(ai_physical_names_in_task_order):
                        cfg_for_current_ai = ai_channels_to_read[phys_name]
                        data,avg=read_ai_data[i],np.mean(read_ai_data[i])
                        min_lim,max_lim,u_name=float(cfg_for_current_ai.get("min_v",-10.0)),float(cfg_for_current_ai.get("max_v",10.0)),cfg_for_current_ai.get("name",phys_name)
                        log_q(f"  Kanava {u_name} ({phys_name}): Keskiarvo={avg:.3f}V (Näytteitä: {len(data)}), Rajat: [{min_lim:.3f}V, {max_lim:.3f}V]")
                        if not (min_lim <= avg <= max_lim):log_q(f"    VIRHE: Arvo ei ole rajojen sisällä!",error=True);test_success=False
                        else:log_q(f"    OK: Arvo rajojen sisällä.")
            if task_ai: task_ai.close(); task_ai = None
        if stop_event.is_set(): raise Exception("Testi keskeytetty AI-lukemisen jälkeen")
        log_q("Kaikki DAQ-toiminnot suoritettu.")

    except nidaqmx.DaqError as e_daq:
        log_q(f"NI-DAQmx virhe: {e_daq}", error=True)
        if hasattr(e_daq, 'error_code'): log_q(f"Virhekoodi: {e_daq.error_code}")
        test_success = False
    except Exception as e:
        log_q(f"Yleinen DAQ Worker Virhe: {e}", error=True)
        log_q(traceback.format_exc())
        test_success = False
    finally:
        for task in [task_ao, task_ai, task_do, task_di]:
            if task is not None:
                try: task.close(); log_q(f"Suljettu DAQ Task: {task.name}")
                except: pass

        q('release_daq_lock', {'device_index': device_index})
        log_q("DAQ-lukko vapautuspyyntö lähetetty.")

        if stop_event.is_set():
            log_q("DAQ-testi lopetettu keskeytyksen vuoksi.")
            q('status', 'DAQ Keskeyt.', 'orange')
            test_success = False
        elif test_success:
            log_q("DAQ-testi suoritettu: OK.")
            q('status', 'DAQ OK', 'green')
        else:
            log_q("DAQ-testi suoritettu: VIRHE.", error=True)
            q('status', 'DAQ Virhe', 'red')

        q('daq_test_done', test_success)

# ... (OpticalInspectionConfigWindow ja TestResultsUI luokat sekä TestiOhjelmaApp-luokan alkuosa pysyvät ennallaan)...

class TestiOhjelmaApp:
    def __init__(self, root):
        self.root = root
        self.root.title("XORTEST v2.4.8")
        self.root.geometry("1350x900")
        self.root.minsize(1000, 700)
        self.root.grid_rowconfigure(0, weight=1)
        self.root.grid_columnconfigure(0, weight=1)

        self.gui_queue = queue.Queue()
        self.default_retry_delay_s = 2.0
        self.test_order: List[Dict[str,Any]] = self._get_default_test_order()

        self._daq_settings_template = self._get_default_daq_settings()
        self._modbus_sequence_template = self._get_default_modbus_sequence()
        self._serial_settings_template = self._get_default_serial_settings()
        self._wait_info_settings_template = {"message": "Odota...", "wait_seconds": 0.0}
        self._optical_inspection_settings_template = self._get_default_optical_inspection_settings() # LISÄYS

        self.step_specific_settings: Dict[str, Any] = {}

        self.bootloader_path = ctk.StringVar()
        self.partitions_path = ctk.StringVar()
        self.app_path = ctk.StringVar()

        self.flash_baudrate = ctk.StringVar(value=DEFAULT_BAUD_RATE_FLASH)
        self.serial_baudrate = ctk.StringVar(value=DEFAULT_BAUD_RATE_SERIAL)
        self.modbus_baudrate = ctk.StringVar(value=str(DEFAULT_MODBUS_BAUDRATE))
        self.modbus_timeout = ctk.StringVar(value=str(DEFAULT_MODBUS_TIMEOUT))

        self.flash_in_sequence_var = ctk.BooleanVar(value=True)

        self.current_device_count = 1
        self.device_names_vars: Dict[int, ctk.StringVar] = {}
        self.flash_ports: Dict[int, ctk.StringVar] = {}
        self.monitor_ports: Dict[int, ctk.StringVar] = {}
        self.modbus_ports: Dict[int, ctk.StringVar] = {}
        self.modbus_slave_ids: Dict[int, ctk.StringVar] = {}
        self.test_selection_vars: Dict[Tuple[int, str], ctk.BooleanVar] = {}

        self.shared_flash_queues: Dict[str, List[int]] = {}
        self.device_state: Dict[int, Dict[str, Any]] = {}
        self.active_threads: Dict[Tuple[int, str], threading.Thread] = {}
        self.stop_events: Dict[int, threading.Event] = {}
        self.daq_lock = threading.Lock()
        self.daq_in_use_by_device: Optional[int] = None
        self.daq_wait_queue: List[Tuple[int, threading.Event]] = []
        self.device_frames: Dict[int, ctk.CTkFrame] = {}
        self.log_text_widgets: Dict[int, ctk.CTkTextbox] = {}
        self.device_log_buffer: Dict[int, List[Tuple[str, str]]] = {}
        self.current_test_run_log_filename: Optional[str] = None

        self._create_widgets()
        self._ensure_test_order_attributes()
        self._ensure_step_specific_settings()
        self._initialize_device_states()
        self.refresh_ports()
        self._update_results_ui_layout()

        self.root.after(100, self._process_gui_queue)

    def _get_default_optical_inspection_settings(self) -> Dict: # LISÄYS
        return {
            "webcam": "Oletuskamera",
            "api_key": "",
            "model_path": "",
            "inspection_target": "ESP32-moduuli",
            "expected_outcome": "Asennettu oikein",
        }

    def _get_default_settings_for_step_type(self, step_type: str) -> Any:
        if step_type == 'serial': return copy.deepcopy(self._serial_settings_template)
        elif step_type == 'daq': return copy.deepcopy(self._daq_settings_template)
        elif step_type == 'modbus': return copy.deepcopy(self._modbus_sequence_template)
        elif step_type == 'wait_info': return copy.deepcopy(self._wait_info_settings_template)
        elif step_type == 'optical_inspection': return copy.deepcopy(self._optical_inspection_settings_template) # LISÄYS
        elif step_type == 'daq_and_serial':
            return {
                'daq_settings': copy.deepcopy(self._daq_settings_template),
                'serial_settings': copy.deepcopy(self._serial_settings_template)
            }
        elif step_type == 'daq_and_modbus':
            return {
                'daq_settings': copy.deepcopy(self._daq_settings_template),
                'modbus_settings': copy.deepcopy(self._modbus_sequence_template)
            }
        return {}

    def _create_left_panel(self):
        left_tabview = ctk.CTkTabview(self.left_frame)
        left_tabview.grid(row=0, column=0, sticky="nsew", padx=0, pady=0)

        config_tab_base_frame = left_tabview.add("Konfiguraatio")
        config_tab_base_frame.grid_rowconfigure(0, weight=1)
        config_tab_base_frame.grid_columnconfigure(0, weight=1)

        run_mode_tab_frame = left_tabview.add("Ajotila")
        run_mode_tab_frame.grid_columnconfigure(0, weight=1)
        run_mode_tab_frame.grid_rowconfigure(0, weight=0)
        run_mode_tab_frame.grid_rowconfigure(1, weight=0)
        run_mode_tab_frame.grid_rowconfigure(2, weight=0)

        designer_tab_frame = left_tabview.add("Testisuunnittelu")
        designer_tab_frame.grid_columnconfigure(0, weight=1)
        designer_tab_frame.grid_rowconfigure(0, weight=1)

        self.test_designer_ui = TestDesigner(designer_tab_frame, self)
        self.test_designer_ui.grid(row=0, column=0, sticky="nsew")

        scroll_content_frame = ctk.CTkScrollableFrame(config_tab_base_frame, label_text=None, fg_color="transparent")
        scroll_content_frame.grid(row=0, column=0, sticky="nsew")

        parent_config_tab = scroll_content_frame
        parent_config_tab.grid_columnconfigure(0, weight=1)
        parent_config_tab.grid_rowconfigure(0, weight=0)
        parent_config_tab.grid_rowconfigure(1, weight=0)
        parent_config_tab.grid_rowconfigure(2, weight=1)

        general_settings_container = ctk.CTkFrame(parent_config_tab)
        general_settings_container.grid(row=0, column=0, sticky="ew", padx=10, pady=(10,5))

        paths_container = ctk.CTkFrame(general_settings_container)
        paths_container.pack(fill="x", expand=False, padx=0, pady=(0, 5))
        ctk.CTkLabel(paths_container, text="Tiedostopolut", font=ctk.CTkFont(weight="bold")).pack(anchor="w", padx=10, pady=(5,2))
        paths_frame = ctk.CTkFrame(paths_container)
        paths_frame.pack(fill="x", padx=5, pady=(0,5))
        paths_frame.grid_columnconfigure(1, weight=1)
        ctk.CTkLabel(paths_frame, text="Bootloader:").grid(row=0, column=0, sticky="w", padx=5, pady=2)
        ctk.CTkEntry(paths_frame, textvariable=self.bootloader_path).grid(row=0, column=1, sticky="ew", padx=5, pady=2)
        ctk.CTkButton(paths_frame, text="...", width=30, command=lambda: self._browse_file(self.bootloader_path)).grid(row=0, column=2, padx=5, pady=2)
        ctk.CTkLabel(paths_frame, text="Partitions:").grid(row=1, column=0, sticky="w", padx=5, pady=2)
        ctk.CTkEntry(paths_frame, textvariable=self.partitions_path).grid(row=1, column=1, sticky="ew", padx=5, pady=2)
        ctk.CTkButton(paths_frame, text="...", width=30, command=lambda: self._browse_file(self.partitions_path)).grid(row=1, column=2, padx=5, pady=2)
        ctk.CTkLabel(paths_frame, text="App:").grid(row=2, column=0, sticky="w", padx=5, pady=2)
        ctk.CTkEntry(paths_frame, textvariable=self.app_path).grid(row=2, column=1, sticky="ew", padx=5, pady=2)
        ctk.CTkButton(paths_frame, text="...", width=30, command=lambda: self._browse_file(self.app_path)).grid(row=2, column=2, padx=5, pady=2)

        baud_container = ctk.CTkFrame(general_settings_container)
        baud_container.pack(fill="x", expand=False, padx=0, pady=5)
        ctk.CTkLabel(baud_container, text="Yhteysasetukset", font=ctk.CTkFont(weight="bold")).pack(anchor="w", padx=10, pady=(5,2))
        baud_frame = ctk.CTkFrame(baud_container)
        baud_frame.pack(fill="x", padx=5, pady=(0,5))
        ctk.CTkLabel(baud_frame, text="Flash Baud:").grid(row=0, column=0, sticky="w", padx=5, pady=2)
        ctk.CTkComboBox(baud_frame, variable=self.flash_baudrate, values=["115200", "230400", "460800", "921600"], width=120, state='readonly').grid(row=0, column=1, sticky="w", padx=5, pady=2)
        ctk.CTkLabel(baud_frame, text="Serial Baud:").grid(row=1, column=0, sticky="w", padx=5, pady=2)
        ctk.CTkComboBox(baud_frame, variable=self.serial_baudrate, values=["9600", "19200", "38400", "57600", "115200", "921600"], width=120, state='readonly').grid(row=1, column=1, sticky="w", padx=5, pady=2)
        ctk.CTkLabel(baud_frame, text="Modbus Baud:").grid(row=2, column=0, sticky="w", padx=5, pady=2)
        ctk.CTkComboBox(baud_frame, variable=self.modbus_baudrate, values=["9600", "19200", "38400", "57600", "115200"], width=120, state='readonly').grid(row=2, column=1, sticky="w", padx=5, pady=2)
        ctk.CTkLabel(baud_frame, text="Modbus Timeout(s):").grid(row=3, column=0, sticky="w", padx=5, pady=2)
        ctk.CTkEntry(baud_frame, textvariable=self.modbus_timeout, width=60).grid(row=3, column=1, sticky="w", padx=5, pady=2)

        test_specific_actions_container = ctk.CTkFrame(parent_config_tab)
        test_specific_actions_container.grid(row=1, column=0, sticky="ew", padx=10, pady=5)

        settings_container = ctk.CTkFrame(test_specific_actions_container)
        settings_container.pack(fill="x", expand=False, padx=0, pady=(0,5))
        ctk.CTkLabel(settings_container, text="Testivaiheiden Yksilölliset Asetukset", font=ctk.CTkFont(weight="bold")).pack(anchor="w", padx=10, pady=(5,2))
        settings_frame = ctk.CTkFrame(settings_container)
        settings_frame.pack(fill="x", padx=5, pady=(0,5))
        sf_inner = ctk.CTkFrame(settings_frame)
        sf_inner.pack(fill="x", padx=5, pady=5)
        sf_inner.grid_columnconfigure((0,1,2,3), weight=1)
        ctk.CTkButton(sf_inner, text="Sarjatesti...", command=self.open_serial_config).grid(row=0, column=0, padx=2, pady=2, sticky="ew")
        ctk.CTkButton(sf_inner, text="DAQ...", command=self.open_daq_config, state="normal" if NIDAQMX_AVAILABLE else "disabled").grid(row=0, column=1, padx=2, pady=2, sticky="ew")
        ctk.CTkButton(sf_inner, text="Modbus...", command=self.open_modbus_config, state="normal" if PYMODBUS_AVAILABLE else "disabled").grid(row=0, column=2, padx=2, pady=2, sticky="ew")
        ctk.CTkButton(sf_inner, text="Odotus/Info...", command=self.open_wait_info_config).grid(row=0, column=3, padx=2, pady=2, sticky="ew")
        ctk.CTkButton(sf_inner, text="Opt. Tarkastus...", command=self.open_optical_inspection_config).grid(row=1, column=0, padx=2, pady=2, sticky="ew") # LISÄYS

        test_order_container = ctk.CTkFrame(test_specific_actions_container)
        test_order_container.pack(fill="x", expand=False, padx=0, pady=5)
        ctk.CTkLabel(test_order_container, text="Testijärjestys", font=ctk.CTkFont(weight="bold")).pack(anchor="w", padx=10, pady=(5,2))
        test_order_frame = ctk.CTkFrame(test_order_container)
        test_order_frame.pack(fill="x", padx=5, pady=(0,5))
        ctk.CTkButton(test_order_frame, text="Muokkaa Testijärjestystä...", command=self.open_test_order_config).pack(fill="x", padx=5, pady=5)

        devices_outer_container = ctk.CTkFrame(parent_config_tab)
        devices_outer_container.grid(row=2, column=0, sticky="nsew", padx=10, pady=(5,10))
        devices_outer_container.grid_rowconfigure(0, weight=0)
        devices_outer_container.grid_rowconfigure(1, weight=1)
        devices_outer_container.grid_columnconfigure(0, weight=1)

        ctk.CTkLabel(devices_outer_container, text="Laitteet", font=ctk.CTkFont(weight="bold")).grid(row=0, column=0, sticky="w", padx=10, pady=(5,0))

        devices_outer_frame = ctk.CTkFrame(devices_outer_container)
        devices_outer_frame.grid(row=1, column=0, sticky="nsew", padx=0, pady=0)
        devices_outer_frame.grid_rowconfigure(0, weight=0)
        devices_outer_frame.grid_rowconfigure(1, weight=1)
        devices_outer_frame.grid_columnconfigure(0, weight=1)

        dev_control_frame = ctk.CTkFrame(devices_outer_frame)
        dev_control_frame.grid(row=0, column=0, sticky="ew", padx=5, pady=(5,5))
        ctk.CTkButton(dev_control_frame, text="Lisää Laite", command=self.add_device).pack(side="left", padx=5)
        ctk.CTkButton(dev_control_frame, text="Poista Viimeinen", command=self.remove_device).pack(side="left", padx=5)

        self.scrollable_devices_frame = ctk.CTkScrollableFrame(devices_outer_frame, label_text=None)
        self.scrollable_devices_frame.grid(row=1, column=0, sticky="nsew", pady=(0,5))
        self._create_device_frames_widgets()

        file_actions_container = ctk.CTkFrame(run_mode_tab_frame)
        file_actions_container.grid(row=0, column=0, sticky="ew", pady=(10,5), padx=10)
        ctk.CTkLabel(file_actions_container, text="Kokoonpanon Hallinta", font=ctk.CTkFont(weight="bold")).pack(anchor="w", pady=(0,5))
        ctk.CTkButton(file_actions_container, text="Lataa Kokoonpano", command=self.load_all_settings).pack(fill="x", pady=3)
        ctk.CTkButton(file_actions_container, text="Tallenna Kokoonpano", command=self.save_all_settings).pack(fill="x", pady=3)
        ctk.CTkButton(file_actions_container, text="Päivitä Porttilistaus", command=self.refresh_ports).pack(fill="x", pady=3)

        run_control_container = ctk.CTkFrame(run_mode_tab_frame)
        run_control_container.grid(row=1, column=0, sticky="ew", pady=5, padx=10)
        ctk.CTkLabel(run_control_container, text="Testien Suoritus", font=ctk.CTkFont(weight="bold")).pack(anchor="w", pady=(0,5))
        ctk.CTkButton(run_control_container, text="Käynnistä Valitut Testit", command=self.start_tests, fg_color="green").pack(fill="x", pady=3)
        ctk.CTkButton(run_control_container, text="Pysäytä Kaikki", command=self.stop_all_tests, fg_color="red").pack(fill="x", pady=3)

        run_settings_container = ctk.CTkFrame(run_mode_tab_frame)
        run_settings_container.grid(row=2, column=0, sticky="ew", pady=5, padx=10)
        ctk.CTkLabel(run_settings_container, text="Ajon Asetukset", font=ctk.CTkFont(weight="bold")).pack(anchor="w", pady=(0,5))

        self.flash_mode_checkbox = ctk.CTkCheckBox(run_settings_container,
                                                   text="Ohjelmoi laitteet jonossa (jos jaettu portti)",
                                                   variable=self.flash_in_sequence_var)
        self.flash_mode_checkbox.pack(anchor="w", pady=5)
        ctk.CTkLabel(run_settings_container, text="(Valitse salliaksesi samanaikaisen ohjelmoinnin VAIN eri porteille)",
                     font=ctk.CTkFont(size=10)).pack(anchor="w", padx=5)

    def open_optical_inspection_config(self): # LISÄYS
        selection = self._select_test_step_for_config_extended('optical_inspection', "Optisen Tarkastuksen")
        if not selection:
            return

        step_id_to_configure, target_config_key, composite_parent_type = selection

        current_settings = self.step_specific_settings.get(step_id_to_configure, self._get_default_settings_for_step_type('optical_inspection'))

        config_window = OpticalInspectionConfigWindow(self.root, current_settings)
        updated_settings = config_window.get_settings()

        if updated_settings is not None:
            self.step_specific_settings[step_id_to_configure] = updated_settings
            step_name = next((s['name'] for s in self.test_order if s['id'] == step_id_to_configure), "Optinen Tarkastus")
            print(f"'{step_name}' (Optinen Tarkastus) asetukset päivitetty.")

    def _start_specific_test_step(self, device_idx: int, target_step_id: str, is_retry: bool = False):
        # ... (alkuosa ennallaan) ...
        try:
            step_settings = self.step_specific_settings.get(target_step_id, self._get_default_settings_for_step_type(current_test_type))

            if current_test_type == 'flash':
                 # ... (ennallaan) ...
            elif current_test_type == 'serial':
                # ... (ennallaan) ...
            elif current_test_type == 'daq':
                # ... (ennallaan) ...
            elif current_test_type == 'modbus':
                # ... (ennallaan) ...
            elif current_test_type == 'wait_info':
                 # ... (ennallaan) ...
            elif current_test_type == 'optical_inspection': # LISÄYS
                target_func = run_optical_inspection_worker
                current_step_settings = self.step_specific_settings.get(target_step_id,
                                                                      self._get_default_settings_for_step_type(current_test_type))
                args = (device_idx, copy.deepcopy(current_step_settings), stop_event, self.gui_queue)
            elif current_test_type == 'daq_and_serial' or current_test_type == 'daq_and_modbus':
                # ... (ennallaan) ...
            else:
                raise ValueError(f"Tuntematon testityyppi: {current_test_type}")

            if target_func:
                 thread = threading.Thread(target=target_func, args=args, kwargs=kwargs, daemon=True)
                 self.active_threads[(device_idx, target_step_id)] = thread
                 thread.start()
            else:
                 raise Exception(f"Kohdefunktiota ei määritelty testityypille {current_test_type}")
        # ... (loppuosa ennallaan) ...

    def _handle_gui_message(self, message: Dict):
        # ... (alkuosa ennallaan) ...
        try:
            # ... (muut msg_type-käsittelyt ennallaan) ...
            if msg_type.endswith('_done'):
                            # --- Start of insertion for composite test _done handling ---
                            # Check if we have an active step ID for the device that sent the _done message
                            active_step_id_for_done_msg = self.device_state[device_idx].get('current_step_id')
                            if active_step_id_for_done_msg:
                                active_step_config = next((s for s in self.test_order if s['id'] == active_step_id_for_done_msg), None)
                                if active_step_config: # Check if config is found
                                    active_step_type = active_step_config['type']

                                    is_composite_step_active = active_step_type in ['daq_and_serial', 'daq_and_modbus']
                                    # These are _done messages that would be sent by individual workers
                                    is_internal_sub_worker_done_message = msg_type in ['serial_test_done', 'daq_test_done', 'modbus_test_done']

                                    if is_composite_step_active and is_internal_sub_worker_done_message:
                                        # This is a _done message from a sub-worker of an active composite test.
                                        # The composite worker is responsible for the overall step completion.
                                        # So, we should not advance the main sequence here.
                                        # self._log_message(device_idx, f"DEBUG: Internal _done msg '{msg_type}' for active composite step '{active_step_id_for_done_msg}' (type: {active_step_type}). Letting composite worker handle.", 'blue')

                                        temp_done_test_type = msg_type.replace('_done', '')
                                        if temp_done_test_type == 'serial_test': temp_done_test_type = 'serial' # Normalize

                                        # Update busy flags based on which sub-worker finished
                                        # Ensure state and busy_flags exist before trying to modify
                                        state = self.device_state[device_idx]
                                        if temp_done_test_type == 'serial':
                                            if state and 'busy_flags' in state: state['busy_flags']['monitor_port'] = False
                                        elif temp_done_test_type == 'modbus':
                                            if state and 'busy_flags' in state: state['busy_flags']['modbus_port'] = False
                                        # Note: DAQ lock and its busy_flag are handled by 'release_daq_lock' message from daq_test_worker,
                                        # so 'daq_test_done' doesn't need to clear state['busy_flags']['daq'] here.

                                        return # IMPORTANT: Exit before main _done processing for this internal message
                            # --- End of insertion ---
                # ... (alkuosa ennallaan) ...
                done_test_type = msg_type.replace('_done', '')
                if done_test_type == 'serial_test': done_test_type = 'serial'
                # LISÄYS:
                if done_test_type == 'optical_inspection':
                    pass # Ei erityisiä busy flageja tällä hetkellä

                # ... (loppuosa _done-käsittelystä ennallaan, varmista että se kattaa uuden tyypin) ...
        # ... (loppuosa ennallaan) ...

# ... (Kaikki muu koodi, mukaan lukien OpticalInspectionConfigWindow, pysyy ennallaan) ...

[end of main.py]
