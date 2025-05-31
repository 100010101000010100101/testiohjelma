#RINNAKKAIN TOIMINTA TESTAAMATTA

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
    'daq_and_modbus': 'DAQ ja Modbus (Rinnakkain)'    # UUSI
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
            # Tässä kohtaa komposiittitestin pitäisi ilmoittaa pääsilmukalle, että se odottaa DAQ:ia,
            # ja pääsilmukka voisi käynnistää tämän workerin uudelleen, kun DAQ on vapaa.
            # Tämä on monimutkainen osa ja vaatii muutoksia _start_specific_test_step-logiikkaan.
            # Yksinkertaistetaan tässä: jos DAQ ei ole heti vapaa, komposiittitesti epäonnistuu.
            # TAI PAREMPI: käytetään annettua add_to_daq_wait_queue_func-funktiota.
            # Jotta tämä toimisi, tämän workerin täytyy palata ja _start_specific_test_step
            # täytyy osata käsitellä "pending_daq_lock" -tila.
            # TÄMÄ ON VIELÄ KESKENERÄINEN KOHTA KOMPOSIITISSA.
            # Oletetaan toistaiseksi, että jos DAQ ei ole vapaa, odotetaan hetki ja yritetään uudelleen (huono ratkaisu tuotantoon)
            # tai ilmoitetaan epäonnistuminen.
            # KÄYTETÄÄN ANNETTUJA FUNKTIOITA:
            current_daq_user = get_daq_in_use_by_device_func()
            if current_daq_user is not None and current_daq_user != device_index:
                 q('output', f"[DAQ] DAQ varattu laitteella {current_daq_user}. Odotetaan...", color='orange')
                 # Tässä kohtaa komposiittiworkerin pitäisi palata ja _start_specific_test_step
                 # hoitaa jonotuksen. Nyt yksinkertaistetaan ja epäonnistutaan, jos DAQ on varattu.
                 # Tämä on yksi suurimmista haasteista tämän toteutuksessa siististi.
                 # Voisimme välittää eventin, jonka _start_specific_test_step asettaa.
                 # Mutta nyt:
                 q('output', "[KOMPOSIT] DAQ varattu, yhdistelmätestiä ei voi aloittaa nyt.", color='red')
                 # Vapauta GUI:ssa busy flag, jos se on asetettu.
                 app_gui_queue.put({'type': 'composite_test_done', 'device_index': device_index, 'data': False}) # Ilmoita epäonnistuminen
                 return


            # Jos päästiin tänne, lukko on (oletettavasti) saatu tai yritetään saada
            if daq_lock_ref.acquire(blocking=True, timeout=1.0): # Lyhyt timeout, jos yllä oleva logiikka ei riitä
                set_daq_in_use_by_device_func(device_index)
                daq_resource_acquired = True
                q('output', "[DAQ] DAQ-lukko saatu.")
                app_gui_queue.put({'type': 'busy_flag_update', 'device_index': device_index, 'flag_name': 'daq', 'value': True})


                def daq_worker_wrapper():
                    try:
                        run_daq_test_worker(device_index, DAQ_DEVICE_NAME, daq_settings, sub_stop_events['daq'], app_gui_queue)
                        sub_test_results['daq'] = True # Oletus, jos worker ei palauta tulosta
                    except Exception as e_daq_wrap:
                        q('output', f"[DAQ] Wrapper virhe: {e_daq_wrap}", color='red', sub_test='daq')
                        sub_test_results['daq'] = False
                    finally:
                        # DAQ-lukon vapautus tapahtuu run_daq_test_workerin sisällä tai tässä, jos se kaatuu aiemmin
                        if daq_resource_acquired and get_daq_in_use_by_device_func() == device_index:
                            app_gui_queue.put({'type': 'release_daq_lock', 'device_index': device_index, 'data': None})
                            # set_daq_in_use_by_device_func(None) # Tämä tehdään release_daq_lock -käsittelijässä
                            # daq_lock_ref.release()
                            # check_daq_queue_func_from_app()


                daq_thread = threading.Thread(target=daq_worker_wrapper, daemon=True)
                daq_thread.start()
                sub_test_threads.append(daq_thread)
                daq_started_successfully = True
            else:
                q('output', "[DAQ] DAQ-lukon saanti epäonnistui timeoutilla.", color='red')
                app_gui_queue.put({'type': 'composite_test_done', 'device_index': device_index, 'data': False})
                return


        # --- TOISEN OSION KÄYNNISTYS (Serial tai Modbus) ---
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

        # --- ODOTA SÄIKEIDEN PÄÄTTYMISTÄ TAI STOP_EVENTIÄ ---
        start_time = time.time()
        # Komposiittivaiheen kestoa ei ole erikseen määritelty, se riippuu aliworkereista
        # Tai voitaisiin lisätä sille oma duration, jos tarpeen.
        # Odotetaan, että kaikki käynnistetyt säikeet päättyvät.
        # Pää-stop_event pysäyttää tämän workerin, joka sitten pysäyttää aliworkerit.

        while any(t.is_alive() for t in sub_test_threads):
            if stop_event.is_set():
                q('output', f"Yhdistelmätesti {composite_type} keskeytetään...", color='orange')
                if daq_thread and daq_thread.is_alive(): sub_stop_events['daq'].set()
                if other_thread and other_thread.is_alive(): sub_stop_events[other_test_type].set()
                overall_success = False
                break
            time.sleep(0.1)

        # Varmista, että kaikki säikeet ovat varmasti päättyneet (lyhyt join timeout)
        for t in sub_test_threads:
            t.join(timeout=2.0)
            if t.is_alive():
                q('output', f"Ali-säie {t.name} ei pysähtynyt ajoissa!", color='red')
                overall_success = False # Jos jokin ei pysähdy, koko homma failaa

        # --- TULOSTEN TARKISTUS ---
        # Tässä vaiheessa `sub_test_results` pitäisi olla täytetty GUI-jonon kautta
        # lähetetyillä `_done` -viesteillä (esim. `daq_test_done` jne.)
        # TAI meidän pitää kerätä ne suoraan tässä wrapperista.
        # Nykyinen `sub_test_results` täyttyy wrapperin finally-lohkossa (jos onnistuu).
        # Parempi olisi, jos wrapperit laittaisivat tuloksen jonoon, josta tämä pääworkeri sen lukisi.

        # Yksinkertaistettu tuloksen tarkistus:
        if not stop_event.is_set(): # Jos ei keskeytetty
            if daq_started_successfully and not sub_test_results.get('daq', False): # Jos DAQ käynnistyi mutta epäonnistui
                q('output', "[DAQ] DAQ-alitesti epäonnistui.", color='red')
                overall_success = False
            if other_thread and not sub_test_results.get(other_test_type, False): # Jos toinen testi käynnistyi mutta epäonnistui
                q('output', f"[{other_test_type.upper()}] {other_test_type}-alitesti epäonnistui.", color='red')
                overall_success = False

            if overall_success:
                 q('output', f"Yhdistelmätesti {composite_type} suoritettu: OK", color='green')
            else:
                 q('output', f"Yhdistelmätesti {composite_type} suoritettu: VIRHE", color='red')
        else: # Keskeytetty
            q('output', f"Yhdistelmätesti {composite_type} keskeytetty.", color='orange')
            overall_success = False


    except Exception as e:
        q('output', f"Kriittinen virhe yhdistelmätestissä ({composite_type}): {e}", color='red')
        q('output', traceback.format_exc(), color='red')
        overall_success = False
    finally:
        # Varmista kaikkien ali-stoppien asetus, jos ei jo tehty
        for sev in sub_stop_events.values(): sev.set()

        # DAQ-lukon vapautus, jos se on vielä hallussa tällä workerilla (turvatoimi)
        if daq_resource_acquired and get_daq_in_use_by_device_func() == device_index:
            app_gui_queue.put({'type': 'release_daq_lock', 'device_index': device_index, 'data': None})

        # Ilmoita pääsovellukselle, että tämä komposiittivaihe on valmis
        # GUI-jonon kautta, jotta _handle_gui_message voi käsitellä sen.
        # Tämän viestin pitäisi olla uniikki, esim. 'composite_test_done'.
        # (Huom: Yllä jo tehtiin tämä tietyissä virhetilanteissa)
        if not overall_success and not stop_event.is_set(): # Jos epäonnistui, mutta ei keskeytetty jo aiemmin
            q('status', f"{composite_type.replace('_',' ')} Virhe", 'red')
        elif stop_event.is_set():
            q('status', f"{composite_type.replace('_',' ')} Keskeyt.", 'orange')
        else: # Onnistui
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
    found_keyword = not keyword # Jos keyword on tyhjä, se katsotaan "löydetyksi" heti
    found_error = False
    try:
        with serial.Serial(port, baudrate, timeout=SERIAL_READ_TIMEOUT) as ser:
            q('output', f"Sarjaportti {port} avattu {baudrate} bps.")
            command_to_send = test_settings.get("command", "")
            if command_to_send: ser.write(command_to_send.encode() + b'\n'); q('output', f"Lähetetty: {command_to_send}")
            
            while time.time() - start_time < duration:
                if stop_event.is_set():
                    q('output', "\n--- Sarjatesti keskeytetty ---")
                    success = False # Varmistetaan keskeytyksen tulos
                    break
                try:
                    line = ser.readline().decode(errors='ignore').strip()
                    if line:
                        q('output', line)
                        
                        # 1. Tarkista avainsana, jos sitä etsitään eikä sitä ole vielä löydetty
                        if keyword and not found_keyword:
                            is_match = (keyword in line) if test_settings.get("case_sensitive", False) else (keyword.lower() in line.lower())
                            if is_match:
                                found_keyword = True
                                q('output', f"Avainsana '{keyword}' LÖYTYI: {line}")
                                q('output', "Avainsana löytyi, lopetetaan sarjamonitorin lukeminen.") # LISÄYS: Selventävä viesti
                                break  # MUUTOS: Lopeta while-looppi heti, kun avainsana löytyy

                        # 2. Tarkista virhesanat (tämä suoritetaan vain, jos avainsana ei löytynyt ja breakannut yllä)
                        for err_s in error_strings:
                            is_err_match = (err_s in line) if test_settings.get("case_sensitive", False) else (err_s.lower() in line.lower())
                            if is_err_match:
                                q('output', f"VIRHE '{err_s}' LÖYTYI: {line}")
                                found_error = True
                                break # Lopeta virhesanojen tarkistus (sisempi for-looppi)
                        if found_error:
                            break # Lopeta while-looppi, koska virhe löytyi
                            
                    else: # Ei riviä luettavissa (timeout), pieni tauko ennen uutta yritystä
                        time.sleep(0.005) 
                        
                except serial.SerialException as se:
                    q('output', f"Sarjaporttivirhe: {se}")
                    found_error = True
                    break # Lopeta while-looppi porttivirheen takia
            # else-haara while-loopille: suoritetaan, jos looppi päättyi normaalisti (duration täyttyi)
            # eikä break-lausekkeen kautta.
            else: 
                if not stop_event.is_set(): # Jos looppi päättyi normaalisti (aika loppui)
                    q('output', "\n--- Sarjatestin maksimikesto saavutettu ---")

        # Testin tuloksen määrittely
        if found_error:
            success = False
            q('output', "\n--- Virhe löydetty sarjatestissä tai porttivirhe ---")
        elif test_settings.get("require_keyword", False) and not found_keyword:
            success = False
            q('output', "\n--- Avainsanaa ei löydetty (vaadittu), vaikka maksimikesto saavutettiin tai lukeminen lopetettiin ---")
        else: # Ei virhettä, ja jos avainsana oli vaadittu, se löytyi.
            success = True

        # Varmistetaan, että stop_eventillä on ylivalta
        if stop_event.is_set():
            success = False
        
        # Lopullinen statusviesti
        if success:
            q('status', 'Serial OK', 'green')
        elif stop_event.is_set(): # Jos pysäytetty, mutta ei välttämättä 'success = False' yllä
            q('status', 'Serial Keskeyt.', 'orange')
        else: # Epäonnistui muusta syystä
            q('status', 'Serial Virhe', 'red')
            
    except serial.SerialException as e:
        q('status', 'Serial Portti Virhe', 'red')
        q('output', f'\n--- Sarjaporttivirhe (avaus epäonnistui): {e} ---')
        success = False
    except Exception as e:
        q('status', 'Serial Yleisvirhe', 'red')
        q('output', f'\n--- Sarjatestin odottamaton virhe: {e} ---')
        traceback.print_exc() # Tulosta traceback konsoliin, auttaa debuggaamisessa
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

        # 1. Analogialähdöt (AO)
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

        # 2. Digitaaliulostulot (DO)
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
            unique_ports_added_to_task = set(); all_do_task_channel_names = [] # Käytä taskin antamia nimiä
            for nidaqmx_port_channel_name_from_settings in do_lines_by_nidaqmx_port.keys(): # "Dev1/port0"
                 if nidaqmx_port_channel_name_from_settings not in unique_ports_added_to_task:
                    try:
                        # Käytä portin nimeä myös taskin kanavanimenä, jos se on yksinkertainen
                        task_channel_do_name = nidaqmx_port_channel_name_from_settings.split('/')[-1] # "port0"
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
                for task_channel_name in all_do_task_channel_names: # Iteroidaan taskiin lisättyjen kanavien nimien kautta
                    # Muodostetaan avain settings-sanakirjaa varten
                    original_port_channel_name_for_settings = f"{daq_device_name}/{task_channel_name}" # Esim. "Dev1/port0"
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

        # 3. Lue digitaalitulot (DI) ja vertaa odotettuihin
        di_lines_to_read_config: Dict[str, Dict] = {}
        for line_name_config, line_cfg_from_settings in dio_settings.items(): # Muutettu line_cfg -> line_cfg_from_settings
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
                    # Käytä uniikkia nimeä taskin sisällä, esim. korvaamalla '/' -> '_'
                    task_channel_di_name = nidaqmx_line_name.replace("/", "_")
                    task_di.di_channels.add_di_chan(nidaqmx_line_name, name_to_assign_to_lines=task_channel_di_name,line_grouping=LineGrouping.CHAN_PER_LINE)
                    log_q(f"  Lisätty DI-kanava: {nidaqmx_line_name} (Task-nimi: {task_channel_di_name})")
                    di_channels_in_task_order.append(nidaqmx_line_name) # Tallenna alkuperäinen fyysinen nimi hakua varten
                except Exception as e: log_q(f"Virhe DI-kanavan lisäyksessä {nidaqmx_line_name}: {e}",error=True);test_success=False;break
            if not test_success: raise Exception("DI-kanavan lisäys epäonnistui.")
            if task_di.di_channels:
                # Lue yksi sample per lisätty kanava. Palauttaa listan, jossa jokainen alkio on boolean.
                read_di_values_list = task_di.read(number_of_samples_per_channel=1)

                if not isinstance(read_di_values_list, list) or len(read_di_values_list) != len(di_channels_in_task_order):
                    log_q(f"Odottamaton muoto tai pituus DI-datalle. Saatu: {read_di_values_list}, Odotettu pituus: {len(di_channels_in_task_order)}", error=True)
                    test_success = False
                else:
                    for i, phys_name in enumerate(di_channels_in_task_order):
                        # KORJAUS: Hae cfg_for_current_di tässä skoopissa
                        cfg_for_current_di = di_lines_to_read_config[phys_name]
                        actual_bool = read_di_values_list[i]
                        # Käytä cfg_for_current_di:tä tästä eteenpäin tälle linjalle
                        exp_str = cfg_for_current_di.get("expected_input","Ignore")
                        u_name = cfg_for_current_di.get("name",phys_name)
                        actual_str="High" if actual_bool else "Low"
                        log_q(f"  Linja {u_name} ({phys_name}): Luettu={actual_str}, Odotettu={exp_str}")
                        if exp_str.lower()!="ignore" and actual_str.lower()!=exp_str.lower():log_q("    VIRHE: Ei täsmää!",error=True);test_success=False
                        elif exp_str.lower()!="ignore":log_q("    OK.")
            if task_di: task_di.close(); task_di = None
        if stop_event.is_set(): raise Exception("Testi keskeytetty DI-lukemisen jälkeen")

        # 4. Lue analogiatulot (AI)
        ai_channels_to_read: Dict[str, Dict] = {f"{daq_device_name}/{ch}": cfg for ch,cfg in settings.get("ai_channels",{}).items() if cfg.get("use")}
        if ai_channels_to_read:
            log_q("Luetaan analogiatulot...")
            task_ai = nidaqmx.Task(f"AI_Task_D{device_index}")
            ai_physical_names_in_task_order = []
            channel_counter_for_name = 0
            for ch_physical_name, ch_cfg_from_settings in ai_channels_to_read.items(): # Muutettu ch_cfg -> ch_cfg_from_settings
                min_val,max_val = float(ch_cfg_from_settings.get("min_v",-10.0)),float(ch_cfg_from_settings.get("max_v",10.0))
                task_channel_name = f"ai_task_chan_{channel_counter_for_name}"; channel_counter_for_name+=1
                try:
                    task_ai.ai_channels.add_ai_voltage_chan(ch_physical_name,name_to_assign_to_channel=task_channel_name,terminal_config=TerminalConfiguration.DEFAULT,min_val=min_val,max_val=max_val)
                    log_q(f"  Lisätty AI-kanava: {ch_physical_name} (Task-nimi: {task_channel_name}, Rajat: {min_val:.2f}V - {max_val:.2f}V)")
                    ai_physical_names_in_task_order.append(ch_physical_name)
                except Exception as e: log_q(f"Virhe AI-kanavan lisäyksessä {ch_physical_name}: {e}",error=True);test_success=False;break
            if not test_success: raise Exception("AI-kanavan lisäys epäonnistui.")
            if task_ai.ai_channels:
                if DAQ_NUM_SAMPLES_PER_CHANNEL <= 0: # Varmistus
                    log_q("DAQ_NUM_SAMPLES_PER_CHANNEL on 0 tai negatiivinen, asetetaan arvoon 1.", error=True)
                    actual_num_samples = 1
                else:
                    actual_num_samples = DAQ_NUM_SAMPLES_PER_CHANNEL

                task_ai.timing.cfg_samp_clk_timing(rate=DAQ_SAMPLE_RATE,sample_mode=AcquisitionType.FINITE,samps_per_chan=actual_num_samples)
                log_q(f"  Näytteenotto: {DAQ_SAMPLE_RATE} Hz, {actual_num_samples} näytettä/kanava.")
                read_ai_data = task_ai.read(number_of_samples_per_channel=actual_num_samples, timeout=10.0)

                # Muunna NumPy arrayksi, JOS se on lista (voi tapahtua jos vain 1 kanava & 1 sample)
                if isinstance(read_ai_data, list):
                    read_ai_data = np.array(read_ai_data)

                # Varmista 2D-muoto, jos vain yksi kanava luettiin
                if read_ai_data.ndim == 1 and len(ai_physical_names_in_task_order) == 1:
                    read_ai_data = read_ai_data.reshape((1, -1)) # Muoto (1, num_samples)
                elif read_ai_data.ndim == 0 and len(ai_physical_names_in_task_order) == 1 and actual_num_samples == 1: # Yksi float-arvo
                    read_ai_data = np.array([[read_ai_data]]) # Muoto (1,1)


                expected_shape = (len(ai_physical_names_in_task_order), actual_num_samples)
                if not isinstance(read_ai_data,np.ndarray) or read_ai_data.shape != expected_shape:
                    log_q(f"Odottamaton muoto tai koko AI-datalle. Saatu shape: {read_ai_data.shape if isinstance(read_ai_data, np.ndarray) else type(read_ai_data)}, Odotettu: {expected_shape}", error=True);test_success=False
                else:
                    log_q(f"  Luettu data (shape): {read_ai_data.shape}")
                    for i,phys_name in enumerate(ai_physical_names_in_task_order):
                        # KORJAUS: Hae cfg_for_current_ai tässä skoopissa
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

def run_modbus_test_worker(device_index: int, modbus_port: str, slave_id: int,
                           test_sequence: List[Dict], baudrate: int, timeout: float,
                           stop_event: threading.Event, app_gui_queue: queue.Queue):
    overall_success = True
    client = None
    # Sanakirja edellisten onnistuneiden lukujen tallentamiseen TÄMÄN worker-ajon sisällä
    # Avain: (address, count), Arvo: List[int] (luetut rekisteriarvot)
    previous_read_values_cache: Dict[Tuple[int, int], List[int]] = {}

    def q(type_str: str, data: Any = None, color: str = 'black'):
        app_gui_queue.put({'type': type_str, 'device_index': device_index, 'data': data, 'status_color': color})

    def log_q(message: str, is_error: bool = False):
        prefix = "ERROR: " if is_error else ""
        q('modbus_log', f"Modbus D{device_index}: {prefix}{message}")

    q('status', "Modbus Testi...", 'orange')
    log_q(f"Aloitetaan Modbus-testi: Portti={modbus_port}, SlaveID={slave_id}, Baud={baudrate}, Timeout={timeout}s")
    log_q(f"Testisekvenssissä {len(test_sequence)} vaihetta.")

    try:
        client = ModbusSerialClient(port=modbus_port, timeout=timeout, baudrate=baudrate)
        if not client.connect():
            log_q(f"Yhteyden muodostus Modbus-laitteeseen epäonnistui portissa {modbus_port}.", is_error=True)
            overall_success = False
        else:
            log_q(f"Yhteys Modbus-laitteeseen OK (Portti: {modbus_port}, Slave: {slave_id}).")

            for i, step in enumerate(test_sequence):
                # (ennallaan oleva koodi stop_event-tarkistukselle ja overall_success-ohitukselle)
                if not overall_success and step.get('action') != "wait": # Wait suoritetaan aina, jos ei stop_event
                    log_q(f"Ohitetaan vaihe {i+1} ({step.get('action', 'Tuntematon')}) aiemman virheen vuoksi.")
                    continue
                if stop_event.is_set():
                    log_q("Modbus-testi keskeytetty käyttäjän toimesta.")
                    overall_success = False; break

                action = step.get('action')
                address = step.get('address')
                step_success = True
                log_q(f"--- Vaihe {i+1}/{len(test_sequence)}: Toiminto={action}, Osoite={address if address != 'N/A' else '-'} ---")

                if action == "wait":
                    # (wait-logiikka ennallaan)
                    duration_ms = step.get('duration_ms', 100)
                    log_q(f"Odotetaan {duration_ms} ms...")
                    time.sleep(duration_ms / 1000.0)
                    step_success = not stop_event.is_set()

                elif action in ["write_register", "write_registers"]:
                    # (write-logiikka ennallaan)
                    value_to_write = step.get('value')
                    if value_to_write is None:
                        log_q(f"Virheellinen arvo kirjoitukselle: {value_to_write}", is_error=True); step_success = False
                    else:
                        log_q(f"Kirjoitetaan rekisteriin {address} arvo {value_to_write}...")
                        try:
                            rr = client.write_register(address=address, value=value_to_write, slave=slave_id)
                            if rr.isError():
                                log_q(f"Modbus-kirjoitusvirhe (FC6): {rr}", is_error=True)
                                if isinstance(rr, ExceptionResponse): log_q(f"  Slave palautti poikkeuksen: {rr.exception_code}")
                                step_success = False
                            else: log_q(f"Kirjoitus onnistui rekisteriin {address}.")
                        except ModbusException as e_mod: log_q(f"Modbus-poikkeus kirjoitettaessa: {e_mod}", is_error=True); step_success = False
                        except Exception as e_gen: log_q(f"Yleinen virhe kirjoitettaessa: {e_gen}\n{traceback.format_exc()}", is_error=True); step_success = False


                elif action in ["read_holding", "read_input"]:
                    count = step.get('count', 1)
                    comparison_mode = step.get('comparison_mode', 'exact')
                    expected_config_str = str(step.get('expected', "Ignore")).strip() # Voi olla arvoja tai offset

                    log_q(f"Luetaan {count} {'holding' if action == 'read_holding' else 'input'} rekisteri(ä) osoitteesta {address}.")
                    log_q(f"  Vertailutapa: {comparison_mode}, Odotettu/Ehto: '{expected_config_str}'")

                    try:
                        if action == "read_holding": read_result = client.read_holding_registers(address=address, count=count, slave=slave_id)
                        else: read_result = client.read_input_registers(address=address, count=count, slave=slave_id)

                        if read_result.isError() or not hasattr(read_result, 'registers') or read_result.registers is None:
                            err_msg = f"Modbus-lukuvirhe tai tyhjä tulos: {read_result}"
                            if hasattr(read_result, 'isError') and read_result.isError() and isinstance(read_result, ExceptionResponse):
                                err_msg += f" (Slave poikkeus: {read_result.exception_code})"
                            log_q(err_msg, is_error=True)
                            step_success = False
                        else:
                            current_read_values = read_result.registers # List[int]
                            log_q(f"Luettu {len(current_read_values)} arvo(a): {current_read_values}")

                            # --- VERTAILULOGIIKKA ---
                            if comparison_mode == "ignore":
                                log_q("Odotettua arvoa ei tarkisteta (Ignore).")
                            elif comparison_mode == "exact":
                                if expected_config_str.lower() == "ignore":
                                    log_q("Odotettua arvoa ei tarkisteta (Ignore via 'exact').")
                                else:
                                    try:
                                        expected_values_int = [int(ev.strip()) for ev in expected_config_str.split(',')]
                                        log_q(f"Odotetut tarkat arvot: {expected_values_int}")
                                        if len(current_read_values) != len(expected_values_int):
                                            log_q(f"Virhe: Luettujen ({len(current_read_values)}) ja odotettujen ({len(expected_values_int)}) arvojen määrä ei täsmää.", is_error=True)
                                            step_success = False
                                        elif current_read_values == expected_values_int:
                                            log_q("Luetut arvot vastaavat tarkkoja odotettuja. OK.")
                                        else:
                                            log_q("Virhe: Luetut arvot EIVÄT vastaa tarkkoja odotettuja.", is_error=True)
                                            for idx, (rv, ev) in enumerate(zip(current_read_values, expected_values_int)):
                                                if rv != ev: log_q(f"  Indeksi {idx}: Luettu={rv}, Odotettu={ev}")
                                            step_success = False
                                    except ValueError:
                                        log_q(f"Virheellinen muoto odotetuissa arvoissa ('exact'): '{expected_config_str}'.", is_error=True)
                                        step_success = False
                            
                            # UUDET VERTAILUTAVAT EDELLISEEN
                            elif "prev_" in comparison_mode:
                                cache_key = (address, count)
                                prev_values_from_cache = previous_read_values_cache.get(cache_key)

                                if prev_values_from_cache is None:
                                    log_q(f"Varoitus: Ei edellistä lukua välimuistissa avaimelle {cache_key}. Ensimmäinen luku.")
                                    # Oletetaan, että ensimmäinen luku on OK, jos vertaillaan edelliseen.
                                    # TAI: voitaisiin määritellä, että tämä on virhe, jos vertailu on pakollinen.
                                    # Nyt: merkitään onnistuneeksi ja tallennetaan arvo.
                                    log_q("  Merkitään onnistuneeksi ja tallennetaan arvot tulevaa vertailua varten.")
                                else:
                                    log_q(f"  Verrataan edellisiin arvoihin ({len(prev_values_from_cache)} kpl): {prev_values_from_cache}")
                                    if len(current_read_values) != len(prev_values_from_cache):
                                        log_q(f"Virhe: Nykyisten ({len(current_read_values)}) ja edellisten ({len(prev_values_from_cache)}) arvojen määrä ei täsmää.", is_error=True)
                                        step_success = False
                                    else:
                                        all_conditions_met = True
                                        for idx, (current_val, prev_val) in enumerate(zip(current_read_values, prev_values_from_cache)):
                                            condition_met_for_val = True
                                            if comparison_mode == "prev_different":
                                                if current_val == prev_val: condition_met_for_val = False
                                            elif comparison_mode == "prev_greater":
                                                if not (current_val > prev_val): condition_met_for_val = False
                                            elif comparison_mode == "prev_less":
                                                if not (current_val < prev_val): condition_met_for_val = False
                                            elif comparison_mode == "prev_equal_offset" or comparison_mode == "prev_different_offset":
                                                try:
                                                    offsets_str_list = expected_config_str.split(',')
                                                    # Jos vain yksi offset, käytä sitä kaikkiin. Muuten käytä vastaavaa offsetia.
                                                    offset_str_for_current = offsets_str_list[idx if idx < len(offsets_str_list) else 0].strip()
                                                    offset_int = int(offset_str_for_current) # Muuntaa "+5" -> 5, "-2" -> -2
                                                    
                                                    target_val = prev_val + offset_int
                                                    if comparison_mode == "prev_equal_offset":
                                                        if current_val != target_val: condition_met_for_val = False
                                                    elif comparison_mode == "prev_different_offset":
                                                        if current_val == target_val: condition_met_for_val = False
                                                except (ValueError, IndexError) as e_off:
                                                    log_q(f"  Virhe offsetin '{expected_config_str}' käsittelyssä indeksille {idx}: {e_off}", is_error=True)
                                                    condition_met_for_val = False; all_conditions_met = False; break # Koko vertailu epäonnistuu
                                            
                                            if not condition_met_for_val:
                                                log_q(f"  Ehto EI TÄYTTYNYT indeksille {idx}: Nykyinen={current_val}, Edellinen={prev_val}, Vertailu='{comparison_mode}', EhtoStr='{expected_config_str}'", is_error=True)
                                                all_conditions_met = False
                                                # break # Voi poistua heti tai tarkistaa kaikki
                                        
                                        if all_conditions_met:
                                            log_q("Kaikki arvot täyttävät vertailuehdon edellisiin. OK.")
                                        else:
                                            step_success = False # Yleinen step_success falseksi

                            else: # Tuntematon comparison_mode
                                log_q(f"Tuntematon vertailutapa: '{comparison_mode}'", is_error=True)
                                step_success = False

                            # Jos lukuvaihe oli onnistunut (kommunikaatio & vertailu), päivitä välimuisti
                            if step_success:
                                previous_read_values_cache[ (address, count) ] = list(current_read_values) # Tallenna kopio!
                                log_q(f"  Päivitetty välimuisti avaimelle {(address, count)} arvoilla: {current_read_values}")
                    
                    except ModbusException as e_mod: log_q(f"Modbus-poikkeus luettaessa: {e_mod}", is_error=True); step_success = False
                    except ValueError as e_val: log_q(f"Arvovirhe (esim. odotettu/offset): {e_val}", is_error=True); step_success = False
                    except Exception as e_gen: log_q(f"Yleinen virhe luettaessa: {e_gen}\n{traceback.format_exc()}", is_error=True); step_success = False
                else:
                    log_q(f"Tuntematon tai tukematon toiminto: {action}", is_error=True)
                    step_success = False

                if not step_success:
                    overall_success = False
                    log_q(f"Vaihe {i+1} epäonnistui.", is_error=True)
                else:
                    log_q(f"Vaihe {i+1} suoritettu: OK.")

            # (stop_event-tarkistus ja overall_success-päivitys ennallaan)
            if stop_event.is_set() and overall_success: overall_success = False


    # (except- ja finally-lohkot ennallaan)
    except ConnectionException as e: log_q(f"Modbus yhteysvirhe: {e}", is_error=True); overall_success = False
    except ModbusIOException as e: log_q(f"Modbus I/O virhe: {e}", is_error=True); overall_success = False
    except Exception as e:
        log_q(f"Odottamaton virhe Modbus-alustuksessa tai testin aikana: {e}", is_error=True)
        log_q(traceback.format_exc()); overall_success = False
    finally:
        if client: client.close(); log_q("Modbus-yhteys suljettu.")

        if stop_event.is_set():
            q('status', 'Modbus Keskeyt.', 'orange'); log_q("Modbus-testi lopullisesti keskeytetty."); overall_success = False
        elif overall_success:
            q('status', 'Modbus OK', 'green'); log_q("Kaikki Modbus-vaiheet suoritettu onnistuneesti.")
        else:
            q('status', 'Modbus Virhe', 'red'); log_q("Modbus-testi epäonnistui tai keskeytyi virheeseen.", is_error=True)
        q('modbus_test_done', overall_success)

def run_wait_info_worker(device_index: int, settings: Dict, stop_event: threading.Event, app_gui_queue: queue.Queue):
    success = True
    message_to_show = settings.get("message", "")
    wait_duration = settings.get("wait_seconds", 5.0)

    def q(type_str: str, data: Any=None, color: str='black'):
         app_gui_queue.put({'type': type_str, 'device_index': device_index, 'data': data, 'status_color': color})

    q('status', f"Odotus/Info: {message_to_show[:30]}...", 'blue')

    if message_to_show:
        q('output', f"--- INFO LAITTEELLE {device_index} ---")
        q('output', message_to_show)
        q('output', "-------------------------")

    if wait_duration > 0:
        q('output', f"Odotetaan {wait_duration:.1f} sekuntia...")
        start_time = time.time()
        while time.time() - start_time < wait_duration:
            if stop_event.is_set():
                q('output', "Odotus keskeytetty.")
                success = False
                break
            time.sleep(0.05)

    if stop_event.is_set():
        q('status', 'Odotus Keskeyt.', 'orange')
    elif success:
        q('status', 'Odotus/Info OK', 'green')

    q('wait_info_done', success)

# === Configuration Windows (Unchanged) ===
class DAQConfigWindow(ctk.CTkToplevel):
     def __init__(self, parent, current_settings: Dict):
        super().__init__(parent)
        self.title("DAQ Testin Asetukset")
        self.resizable(False, False)
        self.parent = parent
        self.grab_set()
        self.transient(parent)
        self.settings = copy.deepcopy(current_settings)
        self.result_settings = None

        self.ai_vars: Dict[str, Dict[str, Any]] = {}
        self.ao_vars: Dict[str, Dict[str, Any]] = {}
        self.dio_vars: Dict[str, Dict[str, Any]] = {}

        self._create_widgets()
        self._load_settings_to_gui()
        self.geometry(f"+{parent.winfo_rootx()+50}+{parent.winfo_rooty()+50}")
        self.protocol("WM_DELETE_WINDOW", self._on_cancel)
        self.wait_window(self)

     def _create_widgets(self):
        main_frame = ctk.CTkFrame(self, corner_radius=0)
        main_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        notebook = ctk.CTkTabview(main_frame)
        notebook.pack(fill="both", expand=True, pady=5)
        notebook.add("AI")
        notebook.add("AO")
        notebook.add("DIO")

        ai_tab_frame = notebook.tab("AI")
        ao_tab_frame = notebook.tab("AO")
        dio_tab_frame = notebook.tab("DIO")

        ai_channels_frame_container = ctk.CTkFrame(ai_tab_frame)
        ai_channels_frame_container.pack(fill="x", expand=True, padx=5, pady=5)
        ctk.CTkLabel(ai_channels_frame_container, text="Kanavat ja Rajat", font=ctk.CTkFont(weight="bold")).pack(anchor="w", padx=5, pady=2)
        ai_channels_frame = ctk.CTkFrame(ai_channels_frame_container)
        ai_channels_frame.pack(fill="x", expand=True, padx=5, pady=2)

        ctk.CTkLabel(ai_channels_frame, text="Kanava").grid(row=0, column=0, padx=5, pady=3, sticky="w")
        ctk.CTkLabel(ai_channels_frame, text="Nimi").grid(row=0, column=1, padx=5, pady=3, sticky="w")
        ctk.CTkLabel(ai_channels_frame, text="Käytä").grid(row=0, column=2, padx=5, pady=3)
        ctk.CTkLabel(ai_channels_frame, text="Min (V)").grid(row=0, column=3, padx=5, pady=3)
        ctk.CTkLabel(ai_channels_frame, text="Max (V)").grid(row=0, column=4, padx=5, pady=3)
        self.ai_vars = {}
        for i in range(8):
            row_idx = i + 1; channel_name = f"ai{i}"
            use_var = ctk.BooleanVar(); min_v_var = ctk.StringVar(value="-10.0"); max_v_var = ctk.StringVar(value="10.0")
            name_var = ctk.StringVar(value=f"Analogitulo {i}")
            ctk.CTkLabel(ai_channels_frame, text=channel_name).grid(row=row_idx, column=0, padx=5, pady=2, sticky="w")
            ctk.CTkEntry(ai_channels_frame, textvariable=name_var, width=120).grid(row=row_idx, column=1, padx=5, pady=2)
            ctk.CTkCheckBox(ai_channels_frame, variable=use_var, text="").grid(row=row_idx, column=2, padx=5, pady=2)
            ctk.CTkEntry(ai_channels_frame, textvariable=min_v_var, width=80).grid(row=row_idx, column=3, padx=5, pady=2)
            ctk.CTkEntry(ai_channels_frame, textvariable=max_v_var, width=80).grid(row=row_idx, column=4, padx=5, pady=2)
            self.ai_vars[channel_name] = {'use': use_var, 'min_v': min_v_var, 'max_v': max_v_var, 'name': name_var}

        ao_channels_frame_container = ctk.CTkFrame(ao_tab_frame)
        ao_channels_frame_container.pack(fill="x", padx=5, pady=5)
        ctk.CTkLabel(ao_channels_frame_container, text="Kanavat & Jännitteet", font=ctk.CTkFont(weight="bold")).pack(anchor="w", padx=5, pady=2)
        ao_channels_frame = ctk.CTkFrame(ao_channels_frame_container)
        ao_channels_frame.pack(fill="x", padx=5, pady=2)

        self.ao_vars = {}
        for i in range(2):
            ch = f"ao{i}"; row_frame = ctk.CTkFrame(ao_channels_frame); row_frame.pack(fill="x", pady=2)
            use_var = ctk.BooleanVar(); output_v_var = ctk.StringVar(value='0.0')
            name_var = ctk.StringVar(value=f"Analogilähtö {i}")
            ctk.CTkLabel(row_frame, text=ch, width=40).pack(side="left", padx=5)
            ctk.CTkLabel(row_frame, text="Nimi:").pack(side="left", padx=5)
            ctk.CTkEntry(row_frame, textvariable=name_var, width=120).pack(side="left", padx=5)
            cb = ctk.CTkCheckBox(row_frame, text="Käytä", variable=use_var)
            cb.pack(side="left", padx=5); ctk.CTkLabel(row_frame, text="Jännite (V):").pack(side="left", padx=5)
            ctk.CTkEntry(row_frame, textvariable=output_v_var, width=80).pack(side="left", padx=5)
            self.ao_vars[ch] = {'use': use_var, 'output_v': output_v_var, 'name': name_var}

        dio_lines_frame_container = ctk.CTkFrame(dio_tab_frame)
        dio_lines_frame_container.pack(fill="x", padx=5, pady=5)
        ctk.CTkLabel(dio_lines_frame_container, text="Linjat", font=ctk.CTkFont(weight="bold")).pack(anchor="w", padx=5, pady=2)
        dio_lines_frame = ctk.CTkFrame(dio_lines_frame_container)
        dio_lines_frame.pack(fill="x", padx=5, pady=2)

        hdrs = ["Linja", "Nimi", "Käytä", "Suunta", "Ulostulo", "Odotettu Tulo"]
        for c, hdr in enumerate(hdrs):
             ctk.CTkLabel(dio_lines_frame, text=hdr, font=ctk.CTkFont(size=12, weight="bold"), anchor="w" if c==0 or c==1 else "center").grid(row=0, column=c, padx=3, pady=3, sticky="w" if c==0 or c==1 else "ew")
        self.dio_vars = {}
        dio_lines = [f"P0.{i}" for i in range(8)] + [f"P1.{i}" for i in range(4)]
        for idx, line in enumerate(dio_lines):
            r = idx + 1; use_var = ctk.BooleanVar(); dir_var = ctk.StringVar(value="Input"); out_var = ctk.StringVar(value="Low"); exp_var = ctk.StringVar(value="Ignore")
            name_var = ctk.StringVar(value=f"Digitaalilinja {line}")
            ctk.CTkLabel(dio_lines_frame, text=line).grid(row=r, column=0, padx=3, pady=1, sticky="w")
            ctk.CTkEntry(dio_lines_frame, textvariable=name_var, width=120).grid(row=r, column=1, padx=3, pady=1, sticky="w")
            use_cb = ctk.CTkCheckBox(dio_lines_frame, variable=use_var, text=""); use_cb.grid(row=r, column=2, padx=3, pady=1)
            dir_cb = ctk.CTkComboBox(dio_lines_frame, variable=dir_var, values=["Input", "Output"], width=100, state='readonly'); dir_cb.grid(row=r, column=3, padx=3, pady=1, sticky="w")
            out_cb = ctk.CTkComboBox(dio_lines_frame, variable=out_var, values=["Low", "High"], width=90, state='disabled'); out_cb.grid(row=r, column=4, padx=3, pady=1, sticky="w")
            exp_cb = ctk.CTkComboBox(dio_lines_frame, variable=exp_var, values=["Ignore", "Low", "High"], width=90, state='readonly'); exp_cb.grid(row=r, column=5, padx=3, pady=1, sticky="w")

            def update_dio_state_closure(direction_var, output_combo, expected_combo):
                def update_dio_state(current_selection=None): 
                    is_output = direction_var.get() == "Output"
                    output_combo.configure(state='normal' if is_output else 'disabled')
                    expected_combo.configure(state='normal' if not is_output else 'disabled') 
                    if is_output: expected_combo.set("Ignore")
                    else: output_combo.set("Low")
                return update_dio_state

            callback = update_dio_state_closure(dir_var, out_cb, exp_cb)
            dir_cb.configure(command=callback)
            callback() 
            self.dio_vars[line] = {'use': use_var, 'direction': dir_var, 'output_val': out_var, 'expected_input': exp_var, 'output_combo': out_cb, 'expected_combo': exp_cb, 'name': name_var}

        bottom_frame = ctk.CTkFrame(main_frame); bottom_frame.pack(fill="x", pady=(10, 0))
        ctk.CTkButton(bottom_frame, text="Tallenna...", command=self._save_settings).pack(side="left", padx=5)
        ctk.CTkButton(bottom_frame, text="Lataa...", command=self._load_settings).pack(side="left", padx=5)
        ctk.CTkButton(bottom_frame, text="Peruuta", command=self._on_cancel).pack(side="right", padx=5)
        ctk.CTkButton(bottom_frame, text="OK", command=self._on_ok, fg_color="green").pack(side="right", padx=5)

     def _load_settings_to_gui(self):
        ai_set = self.settings.get("ai_channels", {})
        ao_set = self.settings.get("ao_channels", {})
        dio_set = self.settings.get("dio_lines", {})

        for ch, vars_dict in self.ai_vars.items():
            ch_settings = ai_set.get(ch, {})
            vars_dict['use'].set(ch_settings.get("use", False))
            vars_dict['min_v'].set(str(ch_settings.get("min_v", -10.0)))
            vars_dict['max_v'].set(str(ch_settings.get("max_v", 10.0)))
            vars_dict['name'].set(ch_settings.get("name", f"Analogitulo {ch[2:]}"))

        for ch, vars_dict in self.ao_vars.items():
            ch_settings = ao_set.get(ch, {})
            vars_dict['use'].set(ch_settings.get("use", False))
            vars_dict['output_v'].set(str(ch_settings.get("output_v", 0.0)))
            vars_dict['name'].set(ch_settings.get("name", f"Analogilähtö {ch[2:]}"))

        for line, vars_dict in self.dio_vars.items():
            ls = dio_set.get(line, {})
            vars_dict['use'].set(ls.get("use", False))
            vars_dict['direction'].set(ls.get("direction", "Input"))
            vars_dict['output_val'].set(ls.get("output_val", "Low"))
            vars_dict['expected_input'].set(ls.get("expected_input", "Ignore"))
            vars_dict['name'].set(ls.get("name", f"Digitaalilinja {line}"))
            
            is_output = vars_dict['direction'].get() == "Output"
            vars_dict['output_combo'].configure(state='normal' if is_output else 'disabled')
            vars_dict['expected_combo'].configure(state='normal' if not is_output else 'disabled')

     def _update_settings_from_gui(self):
        if "ai_channels" not in self.settings: self.settings["ai_channels"] = {}
        for channel, vars_dict in self.ai_vars.items():
            if channel not in self.settings["ai_channels"]: self.settings["ai_channels"][channel] = {}
            self.settings["ai_channels"][channel]["use"] = vars_dict['use'].get()
            self.settings["ai_channels"][channel]["name"] = vars_dict['name'].get()
            try: self.settings["ai_channels"][channel]["min_v"] = float(vars_dict['min_v'].get())
            except ValueError: self.settings["ai_channels"][channel]["min_v"] = -10.0
            try: self.settings["ai_channels"][channel]["max_v"] = float(vars_dict['max_v'].get())
            except ValueError: self.settings["ai_channels"][channel]["max_v"] = 10.0

        if "ao_channels" not in self.settings: self.settings["ao_channels"] = {}
        for channel, vars_dict in self.ao_vars.items():
            if channel not in self.settings["ao_channels"]: self.settings["ao_channels"][channel] = {}
            self.settings["ao_channels"][channel]["use"] = vars_dict['use'].get()
            self.settings["ao_channels"][channel]["name"] = vars_dict['name'].get()
            try: self.settings["ao_channels"][channel]["output_v"] = float(vars_dict['output_v'].get())
            except ValueError: self.settings["ao_channels"][channel]["output_v"] = 0.0

        if "dio_lines" not in self.settings: self.settings["dio_lines"] = {}
        for line, vars_dict in self.dio_vars.items():
            if line not in self.settings["dio_lines"]: self.settings["dio_lines"][line] = {}
            self.settings["dio_lines"][line]["use"] = vars_dict['use'].get()
            self.settings["dio_lines"][line]["name"] = vars_dict['name'].get()
            self.settings["dio_lines"][line]["direction"] = vars_dict['direction'].get()
            self.settings["dio_lines"][line]["output_val"] = vars_dict['output_val'].get()
            self.settings["dio_lines"][line]["expected_input"] = vars_dict['expected_input'].get()

     def _save_settings(self):
        self._update_settings_from_gui()
        fp = filedialog.asksaveasfilename(title="Tallenna DAQ Asetukset", defaultextension=".json", filetypes=(("JSON", "*.json"), ("All", "*.*")), parent=self)
        if not fp: return
        try:
            with open(fp, 'w', encoding='utf-8') as f: json.dump(self.settings, f, indent=4)
            messagebox.showinfo("Tallennettu", f"Asetukset tallennettu:\n{fp}", parent=self)
        except Exception as e: messagebox.showerror("Tallennusvirhe", f"Tallennus epäonnistui:\n{e}", parent=self)

     def _load_settings(self):
         fp = filedialog.askopenfilename(title="Lataa DAQ Asetukset", filetypes=(("JSON", "*.json"), ("All", "*.*")), parent=self)
         if not fp: return
         try:
             with open(fp, 'r', encoding='utf-8') as f: loaded_settings = json.load(f)
             if not isinstance(loaded_settings, dict): raise ValueError("Tiedosto ei ole kelvollinen DAQ-asetustiedosto.")
             if not all(k in loaded_settings for k in ["ai_channels", "ao_channels", "dio_lines"]):
                 print("Warning: Loaded DAQ settings file might be missing some sections.")
             self.settings = copy.deepcopy(loaded_settings)
             self._load_settings_to_gui()
             messagebox.showinfo("Ladattu", f"Asetukset ladattu:\n{fp}", parent=self)
         except json.JSONDecodeError: messagebox.showerror("Latausvirhe", "Tiedosto ei ole kelvollista JSON-muotoa.", parent=self)
         except ValueError as e: messagebox.showerror("Latausvirhe", f"Virheellinen tiedoston sisältö: {e}", parent=self)
         except Exception as e: messagebox.showerror("Latausvirhe", f"Lataaminen epäonnistui:\n{e}", parent=self)

     def _on_ok(self):
        self._update_settings_from_gui()
        self.result_settings = self.settings
        self.destroy()

     def _on_cancel(self):
        self.result_settings = None
        self.destroy()

     def get_settings(self) -> Optional[Dict]:
         return self.result_settings

class ModbusConfigWindow(ctk.CTkToplevel):
     def __init__(self, parent, current_settings: List[Dict]):
        super().__init__(parent)
        self.title("Modbus Testisekvenssin Asetukset")
        self.parent = parent
        self.grab_set()
        self.transient(parent)

        self.sequence = copy.deepcopy(current_settings)
        # Varmista, että kaikilla vaiheilla on uudet vertailuavaimet
        for step in self.sequence:
            step.setdefault('comparison_mode', 'exact') # Oletus 'exact'
            # 'expected' käytetään edelleen tarkalle arvolle tai offsetille
            step.setdefault('expected', step.get('expected', 'Ignore'))


        self.result_sequence = None

        self.add_action_var = ctk.StringVar()
        self.add_addr_var = ctk.StringVar()
        self.add_param_var = ctk.StringVar() # Arvo/Määrä/Viive

        self.add_comparison_mode_var = ctk.StringVar(value="exact")
        # TÄMÄ ON OIKEA ALUSTUS:
        self.add_expected_value_var = ctk.StringVar(value="Ignore")

        self._create_widgets()
        self._populate_tree()

        self.geometry("850x650")
        self.geometry(f"+{parent.winfo_rootx()+60}+{parent.winfo_rooty()+60}")
        self.protocol("WM_DELETE_WINDOW", self._on_cancel)
        self.wait_window(self)

     def _create_widgets(self):
        main_frame = ctk.CTkFrame(self, corner_radius=0)
        main_frame.pack(fill="both", expand=True, padx=10, pady=10)

        tree_frame_container = ctk.CTkFrame(main_frame)
        tree_frame_container.pack(fill="both", expand=True, pady=5)
        ctk.CTkLabel(tree_frame_container, text="Testivaiheet", font=ctk.CTkFont(weight="bold")).pack(anchor="w", padx=5, pady=2)
        tree_frame_content = ctk.CTkFrame(tree_frame_container)
        tree_frame_content.pack(fill="both", expand=True, padx=5, pady=2)

        cols = ("action", "addr", "param", "comparison_mode", "expected_val") # Muutetut sarakkeet
        self.tree = ttk.Treeview(tree_frame_content, columns=cols, show="headings", height=10)
        self.tree.heading("action", text="Toiminto"); self.tree.column("action", width=120, anchor="w")
        self.tree.heading("addr", text="Osoite"); self.tree.column("addr", width=60, anchor="w")
        self.tree.heading("param", text="Param (Arvo/Määrä/Viive ms)"); self.tree.column("param", width=160, anchor="w")
        self.tree.heading("comparison_mode", text="Vertailutapa"); self.tree.column("comparison_mode", width=150, anchor="w")
        self.tree.heading("expected_val", text="Odotettu/Ehto"); self.tree.column("expected_val", width=150, anchor="w")

        tsb_y = ttk.Scrollbar(tree_frame_content, orient="vertical", command=self.tree.yview)
        tsb_x = ttk.Scrollbar(tree_frame_content, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=tsb_y.set, xscrollcommand=tsb_x.set)
        self.tree.grid(row=0, column=0, sticky='nsew'); tsb_y.grid(row=0, column=1, sticky='ns'); tsb_x.grid(row=1, column=0, sticky='ew')
        tree_frame_content.grid_rowconfigure(0, weight=1); tree_frame_content.grid_columnconfigure(0, weight=1)
        self.tree.bind('<<TreeviewSelect>>', self._on_tree_select)

        mod_button_frame = ctk.CTkFrame(main_frame)
        mod_button_frame.pack(fill="x", pady=5)
        # ... (Siirrä, Poista, Päivitä -napit ennallaan) ...
        ctk.CTkButton(mod_button_frame, text="Siirrä Ylös ▲", command=self._move_up).pack(side="left", padx=5)
        ctk.CTkButton(mod_button_frame, text="Siirrä Alas ▼", command=self._move_down).pack(side="left", padx=5)
        ctk.CTkButton(mod_button_frame, text="Poista Valittu", command=self._remove_step).pack(side="left", padx=5)
        ctk.CTkButton(mod_button_frame, text="Päivitä Valittu", command=self._update_step).pack(side="left", padx=5)

        add_frame_container = ctk.CTkFrame(main_frame)
        add_frame_container.pack(fill="x", pady=5)
        ctk.CTkLabel(add_frame_container, text="Lisää / Muokkaa Vaihetta", font=ctk.CTkFont(weight="bold")).pack(anchor="w", padx=5, pady=2)
        add_frame = ctk.CTkFrame(add_frame_container)
        add_frame.pack(fill="x", padx=5, pady=2)

        # Rivi 0: Toiminto
        ctk.CTkLabel(add_frame, text="Toiminto:").grid(row=0, column=0, padx=5, pady=3, sticky="w")
        action_combo = ctk.CTkComboBox(add_frame, variable=self.add_action_var,
                                    values=["write_register", "read_holding", "read_input", "wait"],
                                    width=150, state='readonly', command=self._toggle_fields_based_on_action)
        action_combo.grid(row=0, column=1, padx=5, pady=3, sticky="w")

        # Rivi 1: Osoite ja Parametri
        ctk.CTkLabel(add_frame, text="Osoite:").grid(row=1, column=0, padx=5, pady=3, sticky="w")
        self.addr_entry = ctk.CTkEntry(add_frame, textvariable=self.add_addr_var, width=100)
        self.addr_entry.grid(row=1, column=1, padx=5, pady=3, sticky="w")

        ctk.CTkLabel(add_frame, text="Parametri:").grid(row=1, column=2, padx=5, pady=3, sticky="w")
        self.param_entry = ctk.CTkEntry(add_frame, textvariable=self.add_param_var, width=150)
        self.param_entry.grid(row=1, column=3, padx=5, pady=3, sticky="w")
        self.param_label = ctk.CTkLabel(add_frame, text="(Arvo/Määrä/Viive ms)")
        self.param_label.grid(row=1, column=4, padx=5, pady=3, sticky="w")

        # Rivi 2: Vertailutapa (Comparison Mode)
        ctk.CTkLabel(add_frame, text="Vertailutapa:").grid(row=2, column=0, padx=5, pady=3, sticky="w")
        self.comparison_mode_combo = ctk.CTkComboBox(add_frame, variable=self.add_comparison_mode_var,
                                              values=[
                                                  "exact", "ignore",
                                                  "prev_different", "prev_greater", "prev_less",
                                                  "prev_equal_offset", "prev_different_offset"
                                              ],
                                              width=200, state='readonly', command=self._toggle_fields_based_on_action)
        self.comparison_mode_combo.grid(row=2, column=1, columnspan=2, padx=5, pady=3, sticky="w")

        # Rivi 3: Odotettu Arvo / Ehto
        self.expected_value_label = ctk.CTkLabel(add_frame, text="Odotettu/Ehto:")
        self.expected_value_label.grid(row=3, column=0, padx=5, pady=3, sticky="w")
        self.expected_value_entry = ctk.CTkEntry(add_frame, textvariable=self.add_expected_value_var, width=250)
        self.expected_value_entry.grid(row=3, column=1, columnspan=2, padx=5, pady=3, sticky="w")
        self.expected_value_info_label = ctk.CTkLabel(add_frame, text="(Arvot/Offset/Tyhjä)")
        self.expected_value_info_label.grid(row=3, column=3, columnspan=2, padx=5, pady=3, sticky="w")

        ctk.CTkButton(add_frame, text="Lisää Vaihe", command=self._add_step).grid(row=4, column=1, pady=8, sticky="w")
        ctk.CTkButton(add_frame, text="Tyhjennä Kentät", command=self._clear_add_fields).grid(row=4, column=3, pady=8, sticky="w")

        self._toggle_fields_based_on_action()

        bottom_frame = ctk.CTkFrame(main_frame)
        bottom_frame.pack(fill="x", pady=(10,0))
        # ... (Tallenna, Lataa, Peruuta, OK -napit ennallaan) ...
        ctk.CTkButton(bottom_frame, text="Tallenna Sekvenssi...", command=self._save_sequence).pack(side="left", padx=5)
        ctk.CTkButton(bottom_frame, text="Lataa Sekvenssi...", command=self._load_sequence).pack(side="left", padx=5)
        ctk.CTkButton(bottom_frame, text="Peruuta", command=self._on_cancel).pack(side="right", padx=5)
        ctk.CTkButton(bottom_frame, text="OK", command=self._on_ok, fg_color="green").pack(side="right", padx=5)
     
     def _populate_tree(self):
        for item in self.tree.get_children(): self.tree.delete(item)
        for idx, step_data in enumerate(self.sequence):
            iid = str(idx)
            action = step_data.get('action', 'N/A')
            addr = step_data.get('address', 'N/A') if action != "wait" else "N/A"
            param = ""
            if action == "wait": param = step_data.get('duration_ms', '')
            elif "write" in action: param = step_data.get('value', '')
            elif "read" in action: param = step_data.get('count', '')

            comparison_mode = step_data.get('comparison_mode', 'exact')
            expected_val_display = step_data.get('expected', 'N/A')

            if action != "read_holding" and action != "read_input":
                comparison_mode_display = "N/A"
                expected_val_display = "N/A"
            else:
                comparison_mode_display = comparison_mode
                if comparison_mode == "ignore": expected_val_display = "Ohitetaan"
                elif comparison_mode in ["prev_different", "prev_greater", "prev_less"]:
                    # Näissä "expected"-kenttä ei ole suoraan relevantti käyttäjälle, koska ehto on jo comparison_mode:ssa
                    # Mutta jos expected sisältää jotain, näytetään se.
                    if not expected_val_display or expected_val_display.lower() == "ignore":
                        expected_val_display = "-" # Tai tyhjä
                # Jos offset, expected_val_display näyttää offsetin

            self.tree.insert("", "end", iid=iid, values=(action, addr, param, comparison_mode_display, expected_val_display))   
     
     def _get_selected_iid(self) -> Optional[str]:
         selection = self.tree.selection(); return selection[0] if selection else None
     
     def _clear_add_fields(self):
        self.add_action_var.set("read_holding")
        self.add_addr_var.set("")
        self.add_param_var.set("1")
        self.add_comparison_mode_var.set("exact")
        self.add_expected_value_var.set("Ignore")
        if self.tree.selection():
            self.tree.selection_remove(self.tree.selection())
        self._toggle_fields_based_on_action()
     
     def _on_tree_select(self, event=None):
        iid = self._get_selected_iid()
        if iid is None: return
        try:
            index = int(iid)
            step_data = self.sequence[index]
            action = step_data.get('action', '')
            self.add_action_var.set(action)
            self.add_comparison_mode_var.set(step_data.get('comparison_mode', 'exact'))
            self.add_expected_value_var.set(str(step_data.get('expected', 'Ignore')))

            if action == "wait":
                self.add_addr_var.set("")
                self.add_param_var.set(str(step_data.get('duration_ms', '')))
            else: # Luku tai kirjoitus
                self.add_addr_var.set(str(step_data.get('address', '')))
                if "write" in action:
                    self.add_param_var.set(str(step_data.get('value', '')))
                elif "read" in action:
                    self.add_param_var.set(str(step_data.get('count', '')))
            self._toggle_fields_based_on_action()
        except (ValueError, IndexError):
            self._clear_add_fields()
   
     def _toggle_fields_based_on_action(self, event=None):
        action = self.add_action_var.get()
        comp_mode = self.add_comparison_mode_var.get()

        is_wait = (action == "wait")
        is_read = ("read" in action)
        is_write = ("write" in action)

        self.addr_entry.configure(state='disabled' if is_wait else 'normal')
        self.param_entry.configure(state='normal') # Aina normaali, label muuttuu

        self.comparison_mode_combo.configure(state='normal' if is_read else 'disabled')
        self.expected_value_entry.configure(state='normal' if is_read else 'disabled')

        if is_wait:
            self.param_label.configure(text="(Viive ms)")
            if self.addr_entry.get(): self.add_addr_var.set("") # Tyhjennä osoite waitille
            self.comparison_mode_combo.set("exact") # Nollaa
            self.expected_value_entry.delete(0, "end"); self.expected_value_entry.insert(0, "N/A")
            self.expected_value_info_label.configure(text="(Ei käytössä)")
        elif is_write:
            self.param_label.configure(text="(Kirjoitettava Arvo)")
            self.comparison_mode_combo.set("exact") # Nollaa
            self.expected_value_entry.delete(0, "end"); self.expected_value_entry.insert(0, "N/A")
            self.expected_value_info_label.configure(text="(Ei käytössä)")
        elif is_read:
            self.param_label.configure(text="(Luettava Määrä)")
            if comp_mode == "exact":
                self.expected_value_label.configure(text="Odotetut Arvot:")
                self.expected_value_info_label.configure(text="(Esim. 10,20 tai Ignore)")
                # Älä tyhjennä, käyttäjä voi syöttää arvoja
            elif comp_mode == "ignore":
                self.expected_value_label.configure(text="Odotettu Arvo:")
                self.expected_value_info_label.configure(text="(Ohitetaan)")
                self.add_expected_value_var.set("Ignore")
            elif comp_mode in ["prev_different", "prev_greater", "prev_less"]:
                self.expected_value_label.configure(text="Ehto:")
                self.expected_value_info_label.configure(text="(Kenttä ei käytössä tällä valinnalla)")
                self.add_expected_value_var.set("") # Tai "N/A"
            elif comp_mode in ["prev_equal_offset", "prev_different_offset"]:
                self.expected_value_label.configure(text="Offset Edelliseen:")
                self.expected_value_info_label.configure(text="(Esim. +5, -10. Pilkulla eroteltuna usealle: +1,-2)")
                # Älä tyhjennä, käyttäjä syöttää offsetin
            else: # Tuntematon
                self.expected_value_label.configure(text="Odotettu/Ehto:")
                self.expected_value_info_label.configure(text="")
        else: # Tuntematon action
             self.param_label.configure(text="(Arvo/Määrä/Viive ms)")

     def _validate_and_get_step_data(self) -> Optional[Dict]:
        action = self.add_action_var.get()
        if not action: messagebox.showerror("Syöttövirhe", "Valitse toiminto.", parent=self); return None

        step = {"action": action}
        step["comparison_mode"] = self.add_comparison_mode_var.get() if "read" in action else "exact"
        step["expected"] = self.add_expected_value_var.get().strip()

        try:
            if action == "wait":
                param_str = self.add_param_var.get()
                if not param_str: raise ValueError("Odostusaika (ms) puuttuu.")
                step["duration_ms"] = int(param_str)
                if step["duration_ms"] <= 0: raise ValueError("Odostusaika > 0.")
                step["address"] = "N/A"; step["value"]= "N/A"; step["count"]= "N/A"
            else: # Luku tai kirjoitus
                addr_str = self.add_addr_var.get()
                param_str = self.add_param_var.get()
                if not addr_str: raise ValueError("Osoite puuttuu.")
                step["address"] = int(addr_str) # Voi nostaa ValueError
                if step["address"] < 0: raise ValueError("Osoite >= 0.")

                if "write" in action:
                    if not param_str: raise ValueError("Kirjoitettava arvo puuttuu.")
                    step["value"] = int(param_str)
                    step["count"]= 1
                elif "read" in action:
                    if not param_str: raise ValueError("Luettava määrä puuttuu.")
                    step["count"] = int(param_str)
                    if step["count"] <= 0: raise ValueError("Luettava määrä > 0.")
                    step["value"]= "N/A"

                    comp_mode = step["comparison_mode"]
                    expected_input = step["expected"]

                    if comp_mode == "exact":
                        if expected_input.lower() != "ignore":
                            try: [int(v.strip()) for v in expected_input.split(',')]
                            except ValueError: raise ValueError("Odotetut arvot ('exact') virheelliset. Pitäisi olla pilkulla eroteltuja numeroita tai 'Ignore'.")
                    elif comp_mode == "ignore":
                        step["expected"] = "Ignore" # Normalisoidaan
                    elif comp_mode in ["prev_different", "prev_greater", "prev_less"]:
                        # "expected" kenttää ei välttämättä tarvita, mutta se voi olla tyhjä tai sisältää kommentin
                        if not expected_input: step["expected"] = "" # Tyhjä on ok
                    elif comp_mode in ["prev_equal_offset", "prev_different_offset"]:
                        if not expected_input:
                            raise ValueError(f"'{comp_mode}' vaatii offset-arvon 'Odotettu/Ehto'-kenttään.")
                        # Tarkista offset(ien) muoto
                        try:
                            offsets_str = expected_input.split(',')
                            for offset_str_single in offsets_str:
                                offset_str_single = offset_str_single.strip()
                                if not (offset_str_single.startswith(('+', '-')) and offset_str_single[1:].isdigit()) and not offset_str_single.isdigit():
                                    raise ValueError() # Virheellinen muoto
                        except ValueError:
                            raise ValueError(f"Virheellinen offset: '{expected_input}'. Pitäisi olla esim. '+5', '-10' tai '+1,-2'.")
                    else:
                        raise ValueError(f"Tuntematon vertailutapa: {comp_mode}")
                else:
                    raise ValueError(f"Tuntematon toiminto: {action}")
            return step
        except ValueError as e:
            messagebox.showerror("Syöttövirhe", f"Virhe: {e}", parent=self)
            return None
        except Exception as e:
            messagebox.showerror("Virhe", f"Odottamaton virhe vaiheen validoinnissa: {e}", parent=self)
            traceback.print_exc()
            return None
    
     def _add_step(self):
         new_step = self._validate_and_get_step_data()
         if new_step:
             self.sequence.append(new_step)
             self._populate_tree()
             self._clear_add_fields()
    
     def _update_step(self):
        iid = self._get_selected_iid()
        if iid is None:
             messagebox.showwarning("Ei Valintaa", "Valitse päivitettävä vaihe listasta.", parent=self)
             return
        updated_step = self._validate_and_get_step_data()
        if updated_step:
            try:
                index = int(iid)
                # Varmista, että kaikki tarvittavat avaimet ovat mukana päivityksessä
                # (validate_and_get_step_data dovrebbe già farlo)
                self.sequence[index].update(updated_step) # Käytä updatea, jotta vanhat ylimääräiset avaimet eivät katoa, jos niitä on
                self._populate_tree()
                if self.tree.exists(iid):
                    self.tree.selection_set(iid)
                    self.tree.focus(iid)
            except (ValueError, IndexError):
                 messagebox.showerror("Virhe", f"Vaiheen {iid} päivitys epäonnistui.", parent=self)    
   
     def _remove_step(self):
        iid = self._get_selected_iid()
        if iid is None: return
        try:
             index = int(iid); del self.sequence[index]; self._populate_tree()
             self._clear_add_fields()
        except (ValueError, IndexError): print(f"Virhe poistossa: {iid}")

     def _move_item(self, direction: int):
         iid = self._get_selected_iid()
         if iid is None: return
         try:
             idx = int(iid); new_idx = idx + direction; count = len(self.sequence)
             if not (0 <= new_idx < count): return
             item_data = self.sequence.pop(idx); self.sequence.insert(new_idx, item_data); self._populate_tree()
             new_iid = str(new_idx)
             if self.tree.exists(new_iid):
                 self.tree.selection_set(new_iid); self.tree.focus(new_iid); self.tree.see(new_iid)
         except ValueError: print(f"Virhe siirrossa: {iid}")

     def _move_up(self): self._move_item(-1)
   
     def _move_down(self): self._move_item(1)
   
     def _save_sequence(self):
        for step in self.sequence: # Varmista oletusarvot ennen tallennusta
            step.setdefault('comparison_mode', 'exact')
            step.setdefault('expected', step.get('expected', 'Ignore'))

        filepath = filedialog.asksaveasfilename(title="Tallenna Modbus Sekvenssi", defaultextension=".json", filetypes=(("JSON", "*.json"), ("All", "*.*")), parent=self)
        if not filepath: return
        try:
            with open(filepath, 'w', encoding='utf-8') as f: json.dump(self.sequence, f, indent=4)
            messagebox.showinfo("Tallennettu", f"Modbus-sekvenssi tallennettu:\n{filepath}", parent=self)
        except Exception as e: messagebox.showerror("Tallennusvirhe", f"Tallennus epäonnistui:\n{e}", parent=self)

     def _load_sequence(self):
        filepath = filedialog.askopenfilename(title="Lataa Modbus Sekvenssi", filetypes=(("JSON", "*.json"), ("All", "*.*")), parent=self)
        if not filepath: return
        try:
            with open(filepath, 'r', encoding='utf-8') as f: loaded_sequence = json.load(f)
            if not isinstance(loaded_sequence, list) or not all(isinstance(item, dict) for item in loaded_sequence):
                 raise ValueError("Tiedosto ei sisällä kelvollista Modbus-sekvenssilistaa.")

            for step in loaded_sequence: # Lisää oletusarvot vanhoille tiedostoille
                step.setdefault('comparison_mode', 'exact')
                if 'expected' not in step: # Vanhemmissa tiedostoissa ei välttämättä ole 'expected'
                    step['expected'] = 'Ignore'


            self.sequence = copy.deepcopy(loaded_sequence)
            self._populate_tree()
            self._clear_add_fields()
            messagebox.showinfo("Ladattu", f"Modbus-sekvenssi ladattu:\n{filepath}", parent=self)
        except json.JSONDecodeError: messagebox.showerror("Latausvirhe", "Tiedosto ei ole kelvollista JSON-muotoa.", parent=self)
        except ValueError as e: messagebox.showerror("Latausvirhe", f"Virheellinen tiedoston sisältö: {e}", parent=self)
        except Exception as e: messagebox.showerror("Latausvirhe", f"Lataaminen epäonnistui:\n{e}", parent=self)
  
     def _on_ok(self):
        self.result_sequence = self.sequence
        self.destroy()
  
     def _on_cancel(self):
        self.result_sequence = None
        self.destroy()
 
     def get_sequence(self) -> Optional[List[Dict]]:
        return self.result_sequence

class SerialConfigWindow(ctk.CTkToplevel):
    def __init__(self, parent, current_settings: Dict):
        super().__init__(parent)
        self.title("Sarjatestin Määritys ja Analyysi")
        self.resizable(True, False)
        self.parent = parent
        self.grab_set()
        self.transient(parent)
        self.settings = copy.deepcopy(current_settings)
        self.result_settings = None

        self.duration_var = ctk.StringVar()
        self.command_var = ctk.StringVar()
        self.keyword_var = ctk.StringVar()
        self.delimiter_var = ctk.StringVar()
        self.value_type_var = ctk.StringVar()
        self.expected_value_var = ctk.StringVar()
        self.min_value_var = ctk.StringVar()
        self.max_value_var = ctk.StringVar()
        self.error_strings_var = ctk.StringVar()
        self.require_keyword_var = ctk.BooleanVar()
        self.case_sensitive_var = ctk.BooleanVar()

        self._create_widgets()
        self._load_settings_to_gui()
        self._toggle_value_fields()

        self.geometry("650x630") 
        self.geometry(f"+{parent.winfo_rootx()+80}+{parent.winfo_rooty()+80}")
        self.protocol("WM_DELETE_WINDOW", self._on_cancel)
        self.wait_window(self)

    def _create_widgets(self):
        main_scroll_frame = ctk.CTkScrollableFrame(self, label_text=None) 
        main_scroll_frame.pack(fill="both", expand=True)

        main_frame = ctk.CTkFrame(main_scroll_frame, corner_radius=0) 
        main_frame.pack(fill="both", expand=True, padx=10, pady=10)


        desc_label = ctk.CTkLabel(main_frame, text="Määritä sarjatestin parametrit ja odotetut tulokset/virheet.", justify="left")
        desc_label.pack(anchor="w", pady=(0, 10))

        basic_container = ctk.CTkFrame(main_frame)
        basic_container.pack(fill="x", pady=5)
        ctk.CTkLabel(basic_container, text="Perusasetukset", font=ctk.CTkFont(weight="bold")).pack(anchor="w", padx=5, pady=2)
        basic_frame = ctk.CTkFrame(basic_container)
        basic_frame.pack(fill="x", padx=5, pady=2)
        
        dur_frame = ctk.CTkFrame(basic_frame); dur_frame.pack(fill="x", pady=3)
        ctk.CTkLabel(dur_frame, text="Testin Max Kesto (s):", width=150).pack(side="left", padx=5)
        ctk.CTkEntry(dur_frame, textvariable=self.duration_var, width=80).pack(side="left", padx=5)

        cmd_frame = ctk.CTkFrame(basic_frame); cmd_frame.pack(fill="x", pady=3)
        ctk.CTkLabel(cmd_frame, text="Lähetettävä Komento:", width=150).pack(side="left", padx=5)
        ctk.CTkEntry(cmd_frame, textvariable=self.command_var, width=300).pack(side="left", padx=5, fill="x", expand=True)
        ctk.CTkLabel(cmd_frame, text="(tyhjä = ei lähetetä)").pack(side="left", padx=5)

        search_container = ctk.CTkFrame(main_frame)
        search_container.pack(fill="x", pady=5)
        ctk.CTkLabel(search_container, text="Avainsanan Etsintä ja Arvon Poiminta", font=ctk.CTkFont(weight="bold")).pack(anchor="w", padx=5, pady=2)
        search_frame = ctk.CTkFrame(search_container)
        search_frame.pack(fill="x", padx=5, pady=2)

        kw_frame = ctk.CTkFrame(search_frame); kw_frame.pack(fill="x", pady=2)
        ctk.CTkLabel(kw_frame, text="Etsittävä Avainsana:", width=150).pack(side="left", padx=5)
        ctk.CTkEntry(kw_frame, textvariable=self.keyword_var, width=250).pack(side="left", padx=5, fill="x", expand=True)
        ctk.CTkLabel(kw_frame, text="(tyhjä = ei etsitä)").pack(side="left", padx=5)

        del_frame = ctk.CTkFrame(search_frame); del_frame.pack(fill="x", pady=2)
        ctk.CTkLabel(del_frame, text="Erotin:", width=150).pack(side="left", padx=5)
        ctk.CTkEntry(del_frame, textvariable=self.delimiter_var, width=100).pack(side="left", padx=5)
        ctk.CTkLabel(del_frame, text="(Avainsana<erotin>Arvo, tyhjä=loppurivi)").pack(side="left", padx=5)

        ctk.CTkCheckBox(search_frame, text="Vaadi avainsanan löytyminen testin läpäisyyn", variable=self.require_keyword_var).pack(anchor="w", padx=5, pady=(5,2))
        
        analyze_container = ctk.CTkFrame(main_frame)
        analyze_container.pack(fill="x", pady=5)
        ctk.CTkLabel(analyze_container, text="Poimitun Arvon Analysointi (jos avainsana löytyy)", font=ctk.CTkFont(weight="bold")).pack(anchor="w", padx=5, pady=2)
        analyze_frame = ctk.CTkFrame(analyze_container)
        analyze_frame.pack(fill="x", padx=5, pady=2)

        type_frame = ctk.CTkFrame(analyze_frame); type_frame.pack(fill="x", pady=2)
        ctk.CTkLabel(type_frame, text="Arvon Tyyppi:", width=150).pack(side="left", padx=5)
        self.value_type_combo = ctk.CTkComboBox(type_frame, variable=self.value_type_var,
                                             values=["Ei Tarkisteta", "Teksti", "Numero"], width=150, state='readonly',
                                             command=self._toggle_value_fields)
        self.value_type_combo.pack(side="left", padx=5)

        exp_frame = ctk.CTkFrame(analyze_frame); exp_frame.pack(fill="x", pady=2)
        self.expected_label = ctk.CTkLabel(exp_frame, text="Odotettu Teksti:", width=150)
        self.expected_label.pack(side="left", padx=5)
        self.expected_entry = ctk.CTkEntry(exp_frame, textvariable=self.expected_value_var, width=250)
        self.expected_entry.pack(side="left", padx=5, fill="x", expand=True)

        range_frame = ctk.CTkFrame(analyze_frame); range_frame.pack(fill="x", pady=2)
        self.min_label = ctk.CTkLabel(range_frame, text="Min Arvo:", width=150); self.min_label.pack(side="left", padx=5)
        self.min_entry = ctk.CTkEntry(range_frame, textvariable=self.min_value_var, width=100); self.min_entry.pack(side="left", padx=5)
        self.max_label = ctk.CTkLabel(range_frame, text="Max Arvo:", width=80); self.max_label.pack(side="left", padx=15)
        self.max_entry = ctk.CTkEntry(range_frame, textvariable=self.max_value_var, width=100); self.max_entry.pack(side="left", padx=5)

        error_container = ctk.CTkFrame(main_frame)
        error_container.pack(fill="x", pady=5)
        ctk.CTkLabel(error_container, text="Virheen Merkkijonot (aiheuttaa heti epäonnistumisen)", font=ctk.CTkFont(weight="bold")).pack(anchor="w", padx=5, pady=2)
        error_frame = ctk.CTkFrame(error_container)
        error_frame.pack(fill="x", padx=5, pady=2)
        ctk.CTkLabel(error_frame, text="Etsi näitä (pilkulla erotettuna):").pack(anchor="w", padx=5)
        ctk.CTkEntry(error_frame, textvariable=self.error_strings_var, width=500).pack(fill="x", padx=5, pady=2)

        options_frame = ctk.CTkFrame(main_frame)
        options_frame.pack(fill="x", pady=5)
        ctk.CTkCheckBox(options_frame, text="Huomioi kirjainkoko etsinnässä (avainsana & virheet)", variable=self.case_sensitive_var).pack(anchor="w", padx=5)

        file_ops_frame = ctk.CTkFrame(main_frame)
        file_ops_frame.pack(fill="x", pady=(10, 5)) 
        ctk.CTkButton(file_ops_frame, text="Tallenna Asetukset...", command=self._save_serial_settings_file).pack(side="left", padx=5)
        ctk.CTkButton(file_ops_frame, text="Lataa Asetukset...", command=self._load_serial_settings_file).pack(side="left", padx=5)
        
        bottom_ok_cancel_frame = ctk.CTkFrame(main_frame)
        bottom_ok_cancel_frame.pack(fill="x", pady=(5, 10)) 
        ctk.CTkButton(bottom_ok_cancel_frame, text="Peruuta", command=self._on_cancel).pack(side="right", padx=5)
        ctk.CTkButton(bottom_ok_cancel_frame, text="OK", command=self._on_ok, fg_color="green").pack(side="right", padx=5)

    def _toggle_value_fields(self, event=None): 
         value_type = self.value_type_var.get()
         is_numeric = (value_type == "Numero")
         is_text = (value_type == "Teksti")
         
         self.expected_entry.configure(state='normal' if (is_text or is_numeric) else 'disabled')
         self.min_entry.configure(state='normal' if is_numeric else 'disabled')
         self.max_entry.configure(state='normal' if is_numeric else 'disabled')

         if is_text: self.expected_label.configure(text="Odotettu Teksti:")
         elif is_numeric: self.expected_label.configure(text="Tarkka Odotettu Arvo:")
         else: self.expected_label.configure(text="Odotettu Arvo:")

         if not is_numeric:
              self.min_value_var.set("")
              self.max_value_var.set("")
         if not is_text and not is_numeric: 
             self.expected_value_var.set("")

    def _load_settings_to_gui(self): 
        self.duration_var.set(str(self.settings.get("duration_s", 5.0)))
        self.command_var.set(self.settings.get("command", ""))
        self.keyword_var.set(self.settings.get("keyword", ""))
        self.delimiter_var.set(self.settings.get("delimiter", ""))
        self.value_type_var.set(self.settings.get("value_type", "Ei Tarkisteta"))
        self.expected_value_var.set(str(self.settings.get("expected_value", "")))
        self.min_value_var.set(str(self.settings.get("min_value", "")))
        self.max_value_var.set(str(self.settings.get("max_value", "")))
        self.error_strings_var.set(", ".join(self.settings.get("error_strings", [])))
        self.require_keyword_var.set(self.settings.get("require_keyword", False))
        self.case_sensitive_var.set(self.settings.get("case_sensitive", False))
        self._toggle_value_fields()

    def _update_settings_from_gui(self) -> bool: 
        try:
            duration = float(self.duration_var.get())
            if duration <= 0: raise ValueError("Keston täytyy olla > 0.")
            self.settings["duration_s"] = duration
        except ValueError: messagebox.showerror("Virhe", "Virheellinen kesto. Anna positiivinen numero.", parent=self); return False

        self.settings["command"] = self.command_var.get()
        self.settings["keyword"] = self.keyword_var.get().strip()
        self.settings["delimiter"] = self.delimiter_var.get()
        self.settings["value_type"] = self.value_type_var.get()
        self.settings["require_keyword"] = self.require_keyword_var.get()
        self.settings["case_sensitive"] = self.case_sensitive_var.get()

        if self.settings["value_type"] != "Ei Tarkisteta":
             exp_val_str = self.expected_value_var.get().strip()
             min_val_str = self.min_value_var.get().strip()
             max_val_str = self.max_value_var.get().strip()

             self.settings["expected_value"] = exp_val_str
             self.settings["min_value"] = min_val_str
             self.settings["max_value"] = max_val_str

             if self.settings["value_type"] == "Numero":
                 try:
                      if exp_val_str: float(exp_val_str)
                      if min_val_str: float(min_val_str)
                      if max_val_str: float(max_val_str)
                      if min_val_str and max_val_str and float(min_val_str) > float(max_val_str):
                          raise ValueError("Min arvon on oltava pienempi tai yhtäsuuri kuin Max arvo.")
                 except ValueError as e:
                      messagebox.showerror("Virhe", f"Virheellinen numeroarvo:\n{e}", parent=self)
                      return False
        else:
             self.settings["expected_value"] = ""
             self.settings["min_value"] = ""
             self.settings["max_value"] = ""

        error_raw = self.error_strings_var.get()
        self.settings["error_strings"] = [s.strip() for s in error_raw.split(',') if s.strip()]
        return True

    def _on_ok(self): 
        if self._update_settings_from_gui():
            self.result_settings = self.settings
            self.destroy()

    def _on_cancel(self): 
        self.result_settings = None
        self.destroy()

    def get_settings(self) -> Optional[Dict]: 
        return self.result_settings

    def _get_default_serial_settings(self) -> Dict: 
        return {
                "duration_s": 10.0, 
                "command": "", 
                "keyword": "Setup completed in:", 
                "delimiter": " ", 
                "value_type": "Teksti",
                "expected_value": "OK", 
                "min_value": "", 
                "max_value": "", 
                "error_strings": ["", ""],
                "require_keyword": True, 
                "case_sensitive": False
                }

    def _save_serial_settings_file(self):
       if not self._update_settings_from_gui():
           messagebox.showerror("Virhe", "Korjaa virheet asetuksissa ennen tallennusta.", parent=self)
           return
       
       fp = filedialog.asksaveasfilename(
           title="Tallenna Sarjatestin Asetukset",
           defaultextension=".json",
           filetypes=(("JSON", "*.json"), ("All", "*.*")),
           parent=self
       )
       if not fp: return
       try:
           with open(fp, 'w', encoding='utf-8') as f:
               json.dump(self.settings, f, indent=4)
           messagebox.showinfo("Tallennettu", f"Sarjatestin asetukset tallennettu:\n{fp}", parent=self)
       except Exception as e:
           messagebox.showerror("Tallennusvirhe", f"Tallennus epäonnistui:\n{e}", parent=self)

    def _load_serial_settings_file(self):
       fp = filedialog.askopenfilename(
           title="Lataa Sarjatestin Asetukset",
           filetypes=(("JSON", "*.json"), ("All", "*.*")),
           parent=self
       )
       if not fp: return
       try:
           with open(fp, 'r', encoding='utf-8') as f:
               loaded_settings = json.load(f)
           if not isinstance(loaded_settings, dict):
               raise ValueError("Tiedosto ei ole kelvollinen sarjatestin asetustiedosto.")
           
           default_s = self._get_default_serial_settings()
           final_settings = {}
           for key in default_s.keys(): 
               final_settings[key] = loaded_settings.get(key, default_s[key]) 
           
           self.settings = copy.deepcopy(final_settings)
           self._load_settings_to_gui()
           messagebox.showinfo("Ladattu", f"Sarjatestin asetukset ladattu:\n{fp}", parent=self)
       except json.JSONDecodeError:
           messagebox.showerror("Latausvirhe", "Tiedosto ei ole kelvollista JSON-muotoa.", parent=self)
       except ValueError as e:
           messagebox.showerror("Latausvirhe", f"Virheellinen tiedoston sisältö: {e}", parent=self)
       except Exception as e:
           messagebox.showerror("Latausvirhe", f"Lataaminen epäonnistui:\n{e}", parent=self)    
    
class TestOrderConfigWindow(ctk.CTkToplevel):
    def __init__(self, parent, current_test_order: List[Dict[str, Any]], default_retry_delay: float):
        super().__init__(parent)
        self.title("Määritä Testijärjestys ja Uudelleenyritykset")
        self.parent = parent
        self.grab_set()
        self.transient(parent)
        
        self.test_order = copy.deepcopy(current_test_order)
        # Varmista, että kaikilla vaiheilla on oletus retry-avaimet, jos ne puuttuvat
        for step in self.test_order:
            step.setdefault('retry_enabled', False)
            step.setdefault('max_retries', 0)
            step.setdefault('retry_delay_s', default_retry_delay)

        self.result_test_order = None
        self.selected_index_var = ctk.IntVar(value=-1)
        self.default_retry_delay = default_retry_delay # Tallenna oletusviive

        self._create_widgets()
        self._populate_scrollable_list() 

        self.geometry("750x600") # Hieman leveämpi uusille kentille
        self.geometry(f"+{parent.winfo_rootx()+100}+{parent.winfo_rooty()+100}")
        self.protocol("WM_DELETE_WINDOW", self._on_cancel)
        self.wait_window(self)

    def _create_widgets(self):
        main_frame = ctk.CTkFrame(self, corner_radius=0)
        main_frame.pack(fill="both", expand=True, padx=10, pady=10)

        # --- Listanäkymä ---
        list_container_frame = ctk.CTkFrame(main_frame)
        list_container_frame.pack(fill="both", expand=True, pady=5)
        ctk.CTkLabel(list_container_frame, text="Nykyinen järjestys (klikkaa vaihetta valitaksesi):").pack(anchor="w", padx=5)
        
        self.scrollable_list_frame = ctk.CTkScrollableFrame(list_container_frame, label_text=None)
        self.scrollable_list_frame.pack(fill="both", expand=True, pady=5)
        self.list_item_widgets: List[Dict[str, Any]] = [] # Säilyttää widgetit ja niiden muuttujat

        # --- Muokkauspainikkeet listalle ---
        mod_frame = ctk.CTkFrame(main_frame)
        mod_frame.pack(fill="x", pady=5)
        ctk.CTkButton(mod_frame, text="Siirrä Ylös ▲", command=self._move_up).pack(side="left", padx=5)
        ctk.CTkButton(mod_frame, text="Siirrä Alas ▼", command=self._move_down).pack(side="left", padx=5)
        ctk.CTkButton(mod_frame, text="Poista Valittu", command=self._remove_selected).pack(side="left", padx=5)
        ctk.CTkButton(mod_frame, text="Päivitä Valitun Asetukset", command=self._update_selected_step_settings).pack(side="left", padx=5)


        # --- Uuden vaiheen lisäys ---
        add_outer_frame = ctk.CTkFrame(main_frame)
        add_outer_frame.pack(fill="x", pady=(10,5))
        ctk.CTkLabel(add_outer_frame, text="Lisää uusi testivaihe:", font=ctk.CTkFont(weight="bold")).pack(anchor="w", padx=5, pady=(0,2))
        
        add_frame_top = ctk.CTkFrame(add_outer_frame)
        add_frame_top.pack(fill="x", pady=(0,5))

        available_types = list(AVAILABLE_TEST_TYPES.keys())
        self.new_test_type_var = ctk.StringVar(value=available_types[0] if available_types else "")
        ctk.CTkLabel(add_frame_top, text="Tyyppi:").pack(side="left", padx=(5,2))
        ctk.CTkComboBox(add_frame_top, variable=self.new_test_type_var, values=available_types, width=120).pack(side="left", padx=2)
        
        ctk.CTkLabel(add_frame_top, text="Nimi:").pack(side="left", padx=(10,2))
        self.new_test_name_var = ctk.StringVar()
        ctk.CTkEntry(add_frame_top, textvariable=self.new_test_name_var, placeholder_text="Vaiheen nimi").pack(side="left", padx=2, expand=True, fill="x")
        
        add_frame_bottom = ctk.CTkFrame(add_outer_frame)
        add_frame_bottom.pack(fill="x", pady=(0,5))

        self.new_retry_enabled_var = ctk.BooleanVar(value=False)
        ctk.CTkCheckBox(add_frame_bottom, text="Salli uudelleenyritys", variable=self.new_retry_enabled_var, command=self._toggle_new_retry_fields).pack(side="left", padx=5)
        
        ctk.CTkLabel(add_frame_bottom, text="Max yritykset:").pack(side="left", padx=(10,0))
        self.new_max_retries_var = ctk.StringVar(value="0")
        self.new_max_retries_entry = ctk.CTkEntry(add_frame_bottom, textvariable=self.new_max_retries_var, width=40)
        self.new_max_retries_entry.pack(side="left", padx=(0,5))

        ctk.CTkLabel(add_frame_bottom, text="Viive (s):").pack(side="left", padx=(10,0))
        self.new_retry_delay_var = ctk.StringVar(value=str(self.default_retry_delay))
        self.new_retry_delay_entry = ctk.CTkEntry(add_frame_bottom, textvariable=self.new_retry_delay_var, width=50)
        self.new_retry_delay_entry.pack(side="left", padx=(0,5))
        
        ctk.CTkButton(add_frame_bottom, text="Lisää Vaihe", command=self._add_step).pack(side="left", padx=(10,5))
        self._toggle_new_retry_fields() # Aseta kenttien tila alussa


        # --- OK/Peruuta ---
        bottom_frame = ctk.CTkFrame(main_frame)
        bottom_frame.pack(fill="x", pady=(10,0))
        ctk.CTkButton(bottom_frame, text="Peruuta", command=self._on_cancel).pack(side="right", padx=5)
        ctk.CTkButton(bottom_frame, text="OK", command=self._on_ok, fg_color="green").pack(side="right", padx=5)
    def _toggle_new_retry_fields(self):
        state = "normal" if self.new_retry_enabled_var.get() else "disabled"
        self.new_max_retries_entry.configure(state=state)
        self.new_retry_delay_entry.configure(state=state)
        if not self.new_retry_enabled_var.get():
            self.new_max_retries_var.set("0")
            # self.new_retry_delay_var.set(str(self.default_retry_delay)) # Voidaan jättää ennalleen

    def _select_item(self, index: int):
        self.selected_index_var.set(index)
        for i, item_dict in enumerate(self.list_item_widgets):
            btn = item_dict['button']
            is_selected = (i == index)
            
            hover_color = btn._apply_appearance_mode(ctk.ThemeManager.theme["CTkButton"]["hover_color"])
            text_color_selected = "#FFFFFF" if ctk.get_appearance_mode() == "Dark" else "#000000"
            
            current_fg_color = hover_color if is_selected else "transparent"
            current_text_color = text_color_selected if is_selected else btn._apply_appearance_mode(ctk.ThemeManager.theme["CTkLabel"]["text_color"])
            
            # Varmista, ettei hover-väri ole sama kuin normaali fg-väri, jos ei valittu
            if is_selected and hover_color == btn._apply_appearance_mode(ctk.ThemeManager.theme["CTkButton"]["fg_color"]):
                 current_fg_color = btn._apply_appearance_mode(ctk.ThemeManager.theme["CTkFrame"]["fg_color"][1])
            
            btn.configure(fg_color=current_fg_color, text_color=current_text_color)
  

    def _populate_scrollable_list(self):
        for widget_dict in self.list_item_widgets: # Poista vanhat widgetit oikein
            widget_dict['main_frame'].destroy()
        self.list_item_widgets.clear()
        
        current_selection_value = self.selected_index_var.get()
        new_selected_index_after_repopulation = -1


        for i, step_data in enumerate(self.test_order):
            item_widgets = {} # Tähän kerätään tämän itemin widgetit ja muuttujat

            item_main_frame = ctk.CTkFrame(self.scrollable_list_frame, fg_color="transparent")
            item_main_frame.pack(fill="x", pady=2, padx=1)
            item_widgets['main_frame'] = item_main_frame

            # Ylärivi: Valintanappi ja nimi
            top_row_frame = ctk.CTkFrame(item_main_frame, fg_color="transparent")
            top_row_frame.pack(fill="x")

            display_name = step_data.get('name', f"{AVAILABLE_TEST_TYPES.get(step_data['type'], step_data['type'].capitalize())}")
            btn_text = f"{i+1}. {display_name} (Tyyppi: {AVAILABLE_TEST_TYPES.get(step_data['type'], step_data['type'])})"
            
            item_button = ctk.CTkButton(top_row_frame, text=btn_text, anchor="w", fg_color="transparent", hover=False,
                                       text_color=ctk.ThemeManager.theme["CTkLabel"]["text_color"],
                                       command=lambda idx=i: self._select_item(idx))
            item_button.pack(side="left", fill="x", expand=True)
            item_widgets['button'] = item_button

            # Alarivi: Uudelleenyritysasetukset
            bottom_row_frame = ctk.CTkFrame(item_main_frame, fg_color=self.parent.cget("fg_color")) # Tausta kuten parentilla
            bottom_row_frame.pack(fill="x", pady=(0,2), padx=10) # Hieman sisennystä

            retry_enabled_var = ctk.BooleanVar(value=step_data.get('retry_enabled', False))
            item_widgets['retry_enabled_var'] = retry_enabled_var
            retry_checkbox = ctk.CTkCheckBox(bottom_row_frame, text="Uudelleenyritys", variable=retry_enabled_var)
            retry_checkbox.pack(side="left", padx=(0,10))

            max_retries_var = ctk.StringVar(value=str(step_data.get('max_retries', 0)))
            item_widgets['max_retries_var'] = max_retries_var
            ctk.CTkLabel(bottom_row_frame, text="Max yritykset:").pack(side="left", padx=(0,2))
            max_retries_entry = ctk.CTkEntry(bottom_row_frame, textvariable=max_retries_var, width=40)
            max_retries_entry.pack(side="left", padx=(0,10))
            item_widgets['max_retries_entry'] = max_retries_entry
            
            retry_delay_var = ctk.StringVar(value=str(step_data.get('retry_delay_s', self.default_retry_delay)))
            item_widgets['retry_delay_var'] = retry_delay_var
            ctk.CTkLabel(bottom_row_frame, text="Viive (s):").pack(side="left", padx=(0,2))
            retry_delay_entry = ctk.CTkEntry(bottom_row_frame, textvariable=retry_delay_var, width=50)
            retry_delay_entry.pack(side="left", padx=(0,5))
            item_widgets['retry_delay_entry'] = retry_delay_entry

            # Funktio kenttien tilan päivittämiseen tämän itemin checkboxin perusteella
            def create_toggle_command(r_enabled_var, mr_entry, rd_entry):
                def toggle_retry_fields_for_item():
                    state = "normal" if r_enabled_var.get() else "disabled"
                    mr_entry.configure(state=state)
                    rd_entry.configure(state=state)
                    if not r_enabled_var.get():
                        # Voit halutessasi nollata arvot, kun checkbox ei ole valittuna
                        # mr_entry.delete(0, "end"); mr_entry.insert(0, "0")
                        pass 
                return toggle_retry_fields_for_item

            toggle_cmd = create_toggle_command(retry_enabled_var, max_retries_entry, retry_delay_entry)
            retry_checkbox.configure(command=toggle_cmd)
            toggle_cmd() # Aseta alkutila

            self.list_item_widgets.append(item_widgets)
            
            # Jos tämä oli aiemmin valittu item, merkitse se valituksi
            if i == current_selection_value:
                new_selected_index_after_repopulation = i
        
        # Palauta valinta, jos se oli olemassa ja on edelleen validi
        if 0 <= new_selected_index_after_repopulation < len(self.list_item_widgets):
            self._select_item(new_selected_index_after_repopulation)


    def _get_selected_index(self) -> Optional[int]:
        idx = self.selected_index_var.get()
        if 0 <= idx < len(self.test_order):
            return idx
        return None

    def _update_selected_step_settings(self):
        """Päivittää valitun testivaiheen retry-asetukset list_item_widgetsistä test_orderiin."""
        selected_idx = self._get_selected_index()
        if selected_idx is None:
            messagebox.showwarning("Ei Valintaa", "Valitse ensin vaihe listasta päivittääksesi sen asetukset.", parent=self)
            return

        item_data_widgets = self.list_item_widgets[selected_idx]
        step_to_update = self.test_order[selected_idx]

        step_to_update['retry_enabled'] = item_data_widgets['retry_enabled_var'].get()
        try:
            max_r = int(item_data_widgets['max_retries_var'].get())
            delay_s = float(item_data_widgets['retry_delay_var'].get())
            if max_r < 0 or delay_s < 0:
                raise ValueError("Arvojen on oltava positiivisia.")
            step_to_update['max_retries'] = max_r
            step_to_update['retry_delay_s'] = delay_s
            self._log_to_console(f"Päivitetty vaiheen '{step_to_update.get('name')}' retry-asetukset.")
        except ValueError:
            messagebox.showerror("Virheellinen Syöte", "Max yritysten ja viiveen on oltava kelvollisia numeroita (>= 0).", parent=self)
            # Palauta widgettien arvot takaisin data-objektin arvoihin
            item_data_widgets['max_retries_var'].set(str(step_to_update.get('max_retries',0)))
            item_data_widgets['retry_delay_var'].set(str(step_to_update.get('retry_delay_s', self.default_retry_delay)))


    def _move_up(self): self._move_item(-1)
    def _move_down(self): self._move_item(1)

    def _move_item(self, direction: int):
        idx = self._get_selected_index()
        if idx is None: messagebox.showwarning("Ei valintaa", "Valitse ensin vaihe.", parent=self); return
        
        new_idx = idx + direction
        if 0 <= new_idx < len(self.test_order):
            # Päivitä ensin mahdolliset muutokset valitun itemin retry-asetuksiin
            self._update_selected_step_settings_from_widgets_to_data(idx) # Varmista, että tämä metodi on olemassa ja toimii

            self.test_order.insert(new_idx, self.test_order.pop(idx))
            self.selected_index_var.set(new_idx)
            self._populate_scrollable_list()
        else:
            messagebox.showwarning("Siirto Ei Mahdollinen", "Vaihetta ei voi siirtää listan ulkopuolelle.", parent=self)


    def _update_selected_step_settings_from_widgets_to_data(self, index: int):
        """Helper: Lukee UI-kentistä arvot test_order[index]-dataan."""
        if 0 <= index < len(self.list_item_widgets):
            item_widgets = self.list_item_widgets[index]
            step_data = self.test_order[index]
            step_data['retry_enabled'] = item_widgets['retry_enabled_var'].get()
            try:
                step_data['max_retries'] = int(item_widgets['max_retries_var'].get())
                step_data['retry_delay_s'] = float(item_widgets['retry_delay_var'].get())
            except ValueError:
                # Jos arvo on virheellinen, älä päivitä dataa, ehkä näytä virhe tai logaa
                print(f"Varoitus: Virheellinen arvo retry-asetuksissa vaiheelle {index}, ei päivitetty dataan.")
                # Palauta widgetin arvo takaisin vanhaan datan arvoon
                item_widgets['max_retries_var'].set(str(step_data.get('max_retries',0)))
                item_widgets['retry_delay_var'].set(str(step_data.get('retry_delay_s', self.default_retry_delay)))



    def _remove_selected(self):
        idx = self._get_selected_index()
        if idx is None: messagebox.showwarning("Ei valintaa", "Valitse ensin poistettava vaihe.", parent=self); return
        del self.test_order[idx]
        self.selected_index_var.set(-1) 
        self._populate_scrollable_list()

    def _add_step(self):
        test_type = self.new_test_type_var.get()
        if not test_type: 
            messagebox.showwarning("Puuttuva tieto", "Valitse lisättävän testivaiheen tyyppi.", parent=self)
            return
            
        custom_name = self.new_test_name_var.get().strip()
        step_id = str(uuid.uuid4())
        
        if not custom_name:
            type_count = sum(1 for step in self.test_order if step['type'] == test_type) + 1
            display_name = f"{AVAILABLE_TEST_TYPES.get(test_type, test_type.capitalize())} {type_count}"
        else:
            display_name = custom_name

        try:
            max_r = int(self.new_max_retries_var.get())
            delay_s = float(self.new_retry_delay_var.get())
            if max_r < 0 or delay_s < 0: raise ValueError()
        except ValueError:
            messagebox.showerror("Virheellinen Syöte", "Max yritysten ja viiveen on oltava kelvollisia numeroita (>=0).", parent=self)
            return

        new_step = {
            'id': step_id, 
            'type': test_type, 
            'name': display_name,
            'retry_enabled': self.new_retry_enabled_var.get(),
            'max_retries': max_r,
            'retry_delay_s': delay_s
        }
        self.test_order.append(new_step)
        # Valitse juuri lisätty item
        new_idx = len(self.test_order) - 1
        self.selected_index_var.set(new_idx)
        self._populate_scrollable_list()
        # Tyhjennä lisäyskentät
        self.new_test_name_var.set("")
        self.new_retry_enabled_var.set(False)
        self.new_max_retries_var.set("0")
        self.new_retry_delay_var.set(str(self.default_retry_delay))
        self._toggle_new_retry_fields()


    def _on_ok(self):
        # Varmista, että kaikkien vaiheiden asetukset on päivitetty test_orderiin ennen OK:ta
        for i in range(len(self.test_order)):
            self._update_selected_step_settings_from_widgets_to_data(i)
        self.result_test_order = self.test_order
        self.destroy()

    def _on_cancel(self):
        self.result_test_order = None
        self.destroy()

    def get_test_order(self) -> Optional[List[Dict[str, Any]]]:
        return self.result_test_order

    def _log_to_console(self, message: str): # Apufunktio vain tähän ikkunaan
        print(f"TestOrderConfig: {message}")

class WaitInfoConfigWindow(ctk.CTkToplevel):
    def __init__(self, parent, current_settings: Dict):
        super().__init__(parent)
        self.title("Odotus/Info Vaiheen Asetukset")
        self.parent = parent
        self.grab_set()
        self.transient(parent)
        self.settings = copy.deepcopy(current_settings)
        self.result_settings = None

        self.message_var = ctk.StringVar(value=self.settings.get("message", "Odota hetki..."))
        self.wait_seconds_var = ctk.StringVar(value=str(self.settings.get("wait_seconds", 5.0)))

        self._create_widgets()
        
        self.geometry(f"+{parent.winfo_rootx()+150}+{parent.winfo_rooty()+150}")
        self.protocol("WM_DELETE_WINDOW", self._on_cancel)
        self.wait_window(self)

    def _create_widgets(self):
        main_frame = ctk.CTkFrame(self, corner_radius=0)
        main_frame.pack(fill="both", expand=True, padx=10, pady=10)

        ctk.CTkLabel(main_frame, text="Näytettävä viesti:").pack(anchor="w", padx=5, pady=(5,0))
        self.message_entry = ctk.CTkTextbox(main_frame, height=100, wrap="word")
        self.message_entry.pack(fill="x", padx=5, pady=5)
        self.message_entry.insert("1.0", self.message_var.get())

        ctk.CTkLabel(main_frame, text="Odotusaika sekunteina (0 = ei odotusta):").pack(anchor="w", padx=5, pady=(10,0))
        ctk.CTkEntry(main_frame, textvariable=self.wait_seconds_var, width=100).pack(anchor="w", padx=5, pady=5)

        button_frame = ctk.CTkFrame(main_frame, fg_color="transparent")
        button_frame.pack(fill="x", pady=(20,5))
        ctk.CTkButton(button_frame, text="Peruuta", command=self._on_cancel).pack(side="right", padx=5)
        ctk.CTkButton(button_frame, text="OK", command=self._on_ok, fg_color="green").pack(side="right", padx=5)

    def _update_settings_from_gui(self) -> bool:
        self.settings["message"] = self.message_entry.get("1.0", "end-1c").strip() 
        try:
            wait_s = float(self.wait_seconds_var.get())
            if wait_s < 0:
                raise ValueError("Odotusaika ei voi olla negatiivinen.")
            self.settings["wait_seconds"] = wait_s
        except ValueError:
            messagebox.showerror("Virheellinen syöte", "Odotusajan on oltava numero.", parent=self)
            return False
        return True

    def _on_ok(self):
        if self._update_settings_from_gui():
            self.result_settings = self.settings
            self.destroy()

    def _on_cancel(self):
        self.result_settings = None
        self.destroy()

    def get_settings(self) -> Optional[Dict]:
        return self.result_settings

class TestResultsUI(ctk.CTkFrame):
    def __init__(self, parent, app_instance):
        super().__init__(parent)
        self.app = app_instance 
        self.result_canvases: Dict[Tuple[int, str], ctk.CTkCanvas] = {} 
        self.result_texts: Dict[Tuple[int, str], int] = {}
        self.result_ovals: Dict[Tuple[int, str], int] = {} 
        self.device_labels: List[ctk.CTkLabel] = []
        self.test_step_labels: List[ctk.CTkLabel] = [] 
        self.current_device_count = 0

    def create_results_grid(self, device_count: int, device_names: Dict[int, str], current_test_order: List[Dict[str,str]]):
        for widget in self.winfo_children():
            widget.destroy()
        
        self.result_canvases.clear()
        self.result_texts.clear()
        self.result_ovals.clear() 
        self.device_labels.clear()
        self.test_step_labels.clear()
        
        self.current_device_count = device_count

        ctk.CTkLabel(self, text="Testivaihe").grid(row=0, column=0, padx=5, pady=5, sticky="w")

        for i in range(device_count):
            device_idx = i + 1
            dev_name = device_names.get(device_idx, f"Laite {device_idx}")
            lbl = ctk.CTkLabel(self, text=dev_name, anchor="center")
            lbl.grid(row=0, column=device_idx + 1, padx=RESULT_CIRCLE_PADDING, pady=5, sticky="ew")
            self.device_labels.append(lbl)
            self.grid_columnconfigure(device_idx + 1, weight=1)

        for i, test_step_config in enumerate(current_test_order):
            test_step_idx = i + 1 
            test_step_id = test_step_config['id']
            display_name = test_step_config.get('name', f"{AVAILABLE_TEST_TYPES.get(test_step_config['type'], test_step_config['type'].capitalize())}")
            
            lbl = ctk.CTkLabel(self, text=display_name)
            lbl.grid(row=test_step_idx, column=0, padx=5, pady=RESULT_CIRCLE_PADDING, sticky="w")
            self.test_step_labels.append(lbl)

            for c_idx in range(device_count):
                device_idx_for_canvas = c_idx + 1
                canvas_key = (device_idx_for_canvas, test_step_id)
                
                canvas = ctk.CTkCanvas(self, width=RESULT_CIRCLE_SIZE, height=RESULT_CIRCLE_SIZE, highlightthickness=0)
                
                try:
                    parent_bg_color = self.cget("fg_color") 
                    if isinstance(parent_bg_color, tuple) and len(parent_bg_color) == 2:
                         current_mode = ctk.get_appearance_mode()
                         actual_bg = parent_bg_color[0] if current_mode == "Dark" else parent_bg_color[1]
                         canvas.configure(bg=actual_bg)
                    elif isinstance(parent_bg_color, str):
                         canvas.configure(bg=parent_bg_color)
                except Exception as e:
                    print(f"Varoitus: Ei voitu asettaa canvas-taustaväriä dynaamisesti create_results_grid: {e}")
                    fb_bg = "#2B2B2B" if ctk.get_appearance_mode() == "Dark" else "#DCE4EE"
                    try:
                        canvas.configure(bg=fb_bg)
                    except Exception as e2:
                        print(f"Kriittinen virhe canvas-taustavärin asetuksessa: {e2}")


                canvas.grid(row=test_step_idx, column=device_idx_for_canvas + 1, padx=RESULT_CIRCLE_PADDING, pady=RESULT_CIRCLE_PADDING)
                
                oval_tag = f"oval_{device_idx_for_canvas}_{test_step_id.replace('-', '_')}" 
                oval_id = canvas.create_oval(2, 2, RESULT_CIRCLE_SIZE-2, RESULT_CIRCLE_SIZE-2, 
                                             fill=RESULT_NONE_COLOR, outline="grey", width=1, tags=(oval_tag,))
                
                text_id = canvas.create_text(RESULT_TEXT_OFFSET_X, RESULT_TEXT_OFFSET_Y, text="", fill="black", font=('Helvetica', 8, 'bold'))
                
                self.result_canvases[canvas_key] = canvas
                self.result_texts[canvas_key] = text_id
                self.result_ovals[canvas_key] = oval_id 

    def update_device_names(self, device_names: Dict[int, str]):
         for i, lbl in enumerate(self.device_labels):
             device_idx = i + 1
             lbl.configure(text=device_names.get(device_idx, f"Laite {device_idx}"))

    def reset_results(self, current_test_order: List[Dict[str,str]]):
        for device_idx in range(1, self.current_device_count + 1):
            for test_step_config in current_test_order:
                test_step_id = test_step_config['id']
                key = (device_idx, test_step_id)
                if key in self.result_canvases:
                    canvas = self.result_canvases[key]
                    oval_id = self.result_ovals.get(key) 
                    if oval_id:
                        canvas.itemconfig(oval_id, fill=RESULT_NONE_COLOR, outline="grey")
                    
                    text_id = self.result_texts.get(key)
                    if text_id: 
                        canvas.itemconfig(text_id, text="", fill="black")

    def update_test_result(self, device_idx: int, test_step_id: str, result: Optional[bool], running: bool = False):
        if device_idx > self.current_device_count: return

        key = (device_idx, test_step_id)

        if key in self.result_canvases:
            canvas = self.result_canvases[key]
            text_id = self.result_texts.get(key)
            oval_id = self.result_ovals.get(key) 

            color = RESULT_NONE_COLOR
            text = ""
            text_color = "black" 
            outline_color = "grey" 

            if running:
                color = RESULT_RUNNING_COLOR; text = "..."; text_color = "black"; outline_color = "#FF8F00"
            elif result is True:
                color = RESULT_PASS_COLOR; text = "✓"; text_color = "white"; outline_color = "#388E3C"
            elif result is False: 
                color = RESULT_FAIL_COLOR; text = "✗"; text_color = "white"; outline_color = "#D32F2F"
            elif result is None and not running: 
                color = RESULT_SKIP_COLOR; text = "-"; text_color = "white"; outline_color = "#757575" 
            
            if oval_id: 
                try:
                    canvas.itemconfig(oval_id, fill=color, outline=outline_color)
                except Exception as e:
                    print(f"Virhe päivitettäessä ovaalia avaimelle {key}, ID {oval_id}: {e}")
            else:
                print(f"Virhe: Ovaali-itemin ID:tä ei löytynyt avaimelle {key} update_test_result-metodissa.")

            if text_id:
                try:
                    canvas.itemconfig(text_id, text=text, fill=text_color)
                except Exception as e:
                    print(f"Virhe päivitettäessä tekstiä avaimelle {key}, ID {text_id}: {e}")
        else:
            print(f"Virhe: Kanvaasia ei löytynyt laitteelle {device_idx}, testivaiheen ID {test_step_id}, avain {key}")

class SelectStepDialog(ctk.CTkToplevel): 
    def __init__(self, parent, title: str, prompt: str, choices_dict: Dict[str, Any]): # Muutetaan choices_dictin arvon tyyppi Anyksi
        super().__init__(parent)
        self.title(title)
        self.attributes("-topmost", True)
        self.grab_set()
        self.transient(parent)
        # self.result_id: Optional[str] = None # VANHA
        self.selected_key: Optional[str] = None # UUSI: tallentaa valitun NÄYTTÖNIMEN (avaimen)
        self.choices_dict = choices_dict

        ctk.CTkLabel(self, text=prompt, wraplength=380).pack(padx=20, pady=(20, 10))

        self.choice_var = ctk.StringVar()
        display_choices = list(choices_dict.keys()) # Avaimet ovat näyttönimiä
        if display_choices:
            self.choice_var.set(display_choices[0])

        self.combobox = ctk.CTkComboBox(self, variable=self.choice_var, values=display_choices, state="readonly", width=350)
        self.combobox.pack(padx=20, pady=10)

        button_frame = ctk.CTkFrame(self, fg_color="transparent")
        button_frame.pack(pady=(10, 20))
        ctk.CTkButton(button_frame, text="OK", command=self._on_ok, width=100).pack(side="left", padx=10)
        ctk.CTkButton(button_frame, text="Peruuta", command=self._on_cancel, width=100).pack(side="left", padx=10)

        
        parent_x = parent.winfo_x()
        parent_y = parent.winfo_y()
        parent_width = parent.winfo_width()
        parent_height = parent.winfo_height()
        
        self.update_idletasks() 
        dialog_width = self.winfo_width()
        dialog_height = self.winfo_height()
        
        x_pos = parent_x + (parent_width // 2) - (dialog_width // 2)
        y_pos = parent_y + (parent_height // 2) - (dialog_height // 2)
        
        self.geometry(f"+{x_pos}+{y_pos}")
        self.protocol("WM_DELETE_WINDOW", self._on_cancel)
        self.wait_window(self)
  
    def _on_ok(self):
        self.selected_key = self.choice_var.get() # Tallenna valittu näyttönimi
        self.destroy()

    def _on_cancel(self):
        self.selected_key = None
        self.destroy()

    def get_selected_key(self) -> Optional[str]: # UUSI NIMI
        return self.selected_key

class SelectCompositeSubTypeDialog(ctk.CTkToplevel):
    def __init__(self, parent, title: str, prompt: str, main_test_type_display: str, composite_options: List[Tuple[str,str]]):
        # composite_options: lista tupleja [(näyttönimi_komposiitille, komposiitin_tyyppi_avain)]
        # Esim. [("DAQ osio 'DAQ ja Sarjatesti'-vaiheesta", "daq_and_serial")]
        super().__init__(parent)
        self.title(title)
        self.attributes("-topmost", True)
        self.grab_set()
        self.transient(parent)
        self.result: Optional[Tuple[str, Optional[str]]] = None # (valittu_päätyyppi, valittu_komposiittityyppi_jos_tarpeen)

        ctk.CTkLabel(self, text=prompt, wraplength=380).pack(padx=20, pady=(20, 10))

        self.choice_var = ctk.StringVar()
        choices = [f"Itsenäinen {main_test_type_display}-vaihe"]
        self.internal_mapping = {choices[0]: (main_test_type_display.lower(), None)} # Pääavaimen pitäisi olla 'daq', 'serial', jne.

        for display_name, comp_type_key in composite_options:
            choices.append(display_name)
            self.internal_mapping[display_name] = (main_test_type_display.lower(), comp_type_key)

        self.choice_var.set(choices[0])
        ctk.CTkComboBox(self, variable=self.choice_var, values=choices, state="readonly", width=350).pack(padx=20, pady=10)

        button_frame = ctk.CTkFrame(self, fg_color="transparent")
        button_frame.pack(pady=(10, 20))
        ctk.CTkButton(button_frame, text="OK", command=self._on_ok, width=100).pack(side="left", padx=10)
        ctk.CTkButton(button_frame, text="Peruuta", command=self._on_cancel, width=100).pack(side="left", padx=10)
        # (Keskityslogiikka kuten SelectStepDialogissa)
        self.protocol("WM_DELETE_WINDOW", self._on_cancel)
        self.wait_window(self)

    def _on_ok(self):
        selected_display_name = self.choice_var.get()
        self.result = self.internal_mapping.get(selected_display_name)
        self.destroy()

    def _on_cancel(self):
        self.result = None
        self.destroy()

    def get_selection(self) -> Optional[Tuple[str, Optional[str]]]:
        return self.result


class TestiOhjelmaApp:
    def __init__(self, root):
        self.root = root
        self.root.title("XORTEST v2.4.8")
        self.root.geometry("1350x900")
        self.root.minsize(1000, 700)
        self.root.grid_rowconfigure(0, weight=1)
        self.root.grid_columnconfigure(0, weight=1) # Tämä antaa main_frame:lle koko tilan

        self.gui_queue = queue.Queue()
        self.default_retry_delay_s = 2.0 # Oletusviive uudelleenyritysten välillä sekunneissa
        self.test_order: List[Dict[str,Any]] = self._get_default_test_order()

        self._daq_settings_template = self._get_default_daq_settings()
        self._modbus_sequence_template = self._get_default_modbus_sequence()
        self._serial_settings_template = self._get_default_serial_settings()
        self._wait_info_settings_template = {"message": "Odota...", "wait_seconds": 0.0}

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
        self.active_threads: Dict[Tuple[int, str], threading.Thread] = {} # Avain (device_idx, step_id)
        self.stop_events: Dict[int, threading.Event] = {}
        self.daq_lock = threading.Lock()
        self.daq_in_use_by_device: Optional[int] = None
        self.daq_wait_queue: List[Tuple[int, threading.Event]] = [] # (device_idx, event_to_set_when_lock_is_free)
        self.device_frames: Dict[int, ctk.CTkFrame] = {}
        self.log_text_widgets: Dict[int, ctk.CTkTextbox] = {}
        self.device_log_buffer: Dict[int, List[Tuple[str, str]]] = {}
        self.current_test_run_log_filename: Optional[str] = None

        self._create_widgets()
        self._ensure_test_order_attributes() # Varmista retry-avaimet heti alussa
        self._ensure_step_specific_settings()
        self._initialize_device_states()
        self.refresh_ports()
        self._update_results_ui_layout()

        self.root.after(100, self._process_gui_queue)

    def _get_default_test_order(self) -> List[Dict[str,Any]]:
        default_order_items = [
            {'type': 'flash', 'name': 'Ohjelmointi'},
            {'type': 'serial', 'name': 'Sarjatesti'},
        ]
        if NIDAQMX_AVAILABLE:
            default_order_items.append({'type': 'daq', 'name': 'DAQ Testi'})
        if PYMODBUS_AVAILABLE:
            default_order_items.append({'type': 'modbus', 'name': 'Modbus Testi'})
        default_order_items.append({'type': 'wait_info', 'name': 'Odotus/Info Esimerkki'})
        
        final_order = []
        for item in default_order_items:
            final_order.append({
                'id': str(uuid.uuid4()),
                **item,
                'retry_enabled': False, 
                'max_retries': 0,       
                'retry_delay_s': self.default_retry_delay_s 
            })
        return final_order


    def _ensure_test_order_attributes(self):
        for step in self.test_order:
            step.setdefault('retry_enabled', True)
            step.setdefault('max_retries', 3)
            step.setdefault('retry_delay_s', self.default_retry_delay_s)

    def _get_default_settings_for_step_type(self, step_type: str) -> Any:
        if step_type == 'serial': return copy.deepcopy(self._serial_settings_template)
        elif step_type == 'daq': return copy.deepcopy(self._daq_settings_template)
        elif step_type == 'modbus': return copy.deepcopy(self._modbus_sequence_template)
        elif step_type == 'wait_info': return copy.deepcopy(self._wait_info_settings_template)
        elif step_type == 'daq_and_serial': # UUSI
            return {
                'daq_settings': copy.deepcopy(self._daq_settings_template),
                'serial_settings': copy.deepcopy(self._serial_settings_template)
            }
        elif step_type == 'daq_and_modbus': # UUSI
            return {
                'daq_settings': copy.deepcopy(self._daq_settings_template),
                'modbus_settings': copy.deepcopy(self._modbus_sequence_template) # Käytä sequencea täälläkin
            }
        return {}

    def _ensure_step_specific_settings(self):
        current_ids = {s['id'] for s in self.test_order}
        for step_id in list(self.step_specific_settings.keys()): # Iterate over a copy for safe deletion
            if step_id not in current_ids: del self.step_specific_settings[step_id]
        for step_cfg in self.test_order:
            if step_cfg['id'] not in self.step_specific_settings:
                self.step_specific_settings[step_cfg['id']] = self._get_default_settings_for_step_type(step_cfg['type'])

    def _initialize_device_states(self):
        self._ensure_step_specific_settings()
        for i in range(1, self.current_device_count + 1):
            if i not in self.device_state: self.device_state[i] = self._create_default_device_state(i)
            else:
                cfg,rt = self.device_state[i]['config'], self.device_state[i]['runtime']
                cfg['tests_enabled'] = {s['id']: cfg.get('tests_enabled',{}).get(s['id'],False) for s in self.test_order}
                rt['steps_status'] = {s['id']: rt.get('steps_status',{}).get(s['id']) for s in self.test_order}

    def _create_default_device_state(self, device_idx: int) -> Dict[str, Any]:
        # Varmista, että test_orderissa on retry-attribuutit ennen kuin niitä käytetään tässä
        self._ensure_test_order_attributes()
        
        tests_enabled_dict = {step['id']: False for step in self.test_order}
        steps_status_dict = {step['id']: None for step in self.test_order}
        step_attempts_dict = {step['id']: 0 for step in self.test_order}
        return {
            'config': {'name':f"Laite {device_idx}",'flash_port':"",'monitor_port':"",'modbus_port':"",'modbus_slave_id':str(DEFAULT_MODBUS_SLAVE_ID),'tests_enabled':tests_enabled_dict},
            'runtime': {'sequence_running':False,'current_stage_test_step_id':None,'steps_status':steps_status_dict,'step_attempts':step_attempts_dict,'final_result':None,'last_status_msg':"Valmis",'start_time':None,'end_time':None},
            'busy_flags': {'flash_port':False,'monitor_port':False,'modbus_port':False,'daq':False}
        }



    def _get_default_daq_settings(self) -> Dict:
        s={"ai_channels":{},"ao_channels":{},"dio_lines":{}}
        for i in range(8):s["ai_channels"][f"ai{i}"]={"use":False,"min_v":-10.0,"max_v":10.0,"name":f"Analogitulo {i}"}
        for i in range(2):s["ao_channels"][f"ao{i}"]={"use":False,"output_v":0.0,"name":f"Analogilähtö {i}"}
        for l in[f"P0.{i}"for i in range(8)]+[f"P1.{i}"for i in range(4)]:s["dio_lines"][l]={"use":False,"direction":"Input","output_val":"Low","expected_input":"Ignore","name":f"Digitaalilinja {l}"}
        return s

    def _get_default_modbus_sequence(self) -> List[Dict]:
        return [{"action":"write_register","address":100,"value":123,"count":1,"expected":"N/A"},
                {"action":"wait","duration_ms":500,"address":"N/A","value":"N/A","count":"N/A","expected":"N/A"},
                {"action":"read_holding","address":100,"count":1,"expected":"123","value":"N/A"}]

    def _get_default_serial_settings(self) -> Dict:
        return {"duration_s":10.0,"command":"","keyword":"Setup completed in:","delimiter":" ","value_type":"Teksti","expected_value":"OK","min_value":"","max_value":"","error_strings":["",""],"require_keyword":True,"case_sensitive":False}

    def _select_test_step_for_config(self, test_type_to_filter: str, window_title: str) -> Optional[str]:
        # TÄMÄ ON VANHA, KÄYTÄ _select_test_step_for_config_extended
        matching_steps = [s for s in self.test_order if s['type']==test_type_to_filter]
        if not matching_steps: messagebox.showinfo("Ei vaiheita",f"Ei '{AVAILABLE_TEST_TYPES.get(test_type_to_filter,test_type_to_filter)}'-vaiheita.",parent=self.root); return None
        if len(matching_steps)==1: return matching_steps[0]['id']
        # Muuta choices käyttämään uniikkia ID:tä avaimena, jos näyttönimet voivat olla samoja
        choices={f"{s['name']} (ID: ...{s['id'][-6:]})":s['id'] for s in matching_steps} # Pidempi ID-osa
        # Varmista uniikit avaimet dictionaryyn, jos nimet + ID-osatkin menisivät päällekkäin (epätodennäköistä)
        unique_choices = {}
        for s in matching_steps:
            base_name = f"{s['name']} (ID: ...{s['id'][-6:]})"
            name_to_use = base_name
            count = 1
            while name_to_use in unique_choices:
                name_to_use = f"{base_name} ({count})"
                count += 1
            unique_choices[name_to_use] = s['id']

        return SelectStepDialog(self.root,window_title,f"Valitse muokattava '{AVAILABLE_TEST_TYPES.get(test_type_to_filter,test_type_to_filter)}'-vaihe:",unique_choices).get_selected_id()
   
    def _create_widgets(self):
        self.main_frame = ctk.CTkFrame(self.root, corner_radius=0)
        self.main_frame.grid(row=0, column=0, sticky="nsew")
        self.root.grid_rowconfigure(0, weight=1)
        self.root.grid_columnconfigure(0, weight=1)

        # --- PÄÄASIALAINEN JAKO: VASEN PANEELI JA OIKEA ALUE ---
        self.main_paned_window = tk.PanedWindow(self.main_frame, orient=tk.HORIZONTAL, sashwidth=6, sashrelief=tk.RAISED, bg=self.main_frame.cget("fg_color")[1]) # Yritetään sovittaa taustaväri
        self.main_frame.grid_rowconfigure(0, weight=1)
        self.main_frame.grid_columnconfigure(0, weight=1) # PanedWindow täyttää main_frame:n
        self.main_paned_window.grid(row=0, column=0, sticky="nsew")

        # --- VASEN PANEELI (PanedWindowin ensimmäinen osa) ---
        self.left_frame = ctk.CTkFrame(self.main_paned_window, width=400) # Anna lähtöleveys
        self.main_paned_window.add(self.left_frame, minsize=350, stretch="never") # stretch="never" tai "first"
        # Konfiguroidaan left_frame:n sisäinen grid, jotta sen sisältö (tabview) laajenee
        self.left_frame.grid_rowconfigure(0, weight=1)
        self.left_frame.grid_columnconfigure(0, weight=1)

        # --- OIKEA PANEELI (PanedWindowin toinen osa, joka jaetaan edelleen) ---
        self.right_content_area_for_paned = ctk.CTkFrame(self.main_paned_window) # Tämä on välikehys
        self.main_paned_window.add(self.right_content_area_for_paned, minsize=500, stretch="always")
        # Konfiguroidaan tämän välikehyksen sisäinen grid
        self.right_content_area_for_paned.grid_rowconfigure(0, weight=1)
        self.right_content_area_for_paned.grid_columnconfigure(0, weight=1)


        # --- OIKEAN PANEELI SISÄINEN JAKO: TULOKSET JA LOKIT ---
        # Käytetään toista PanedWindowia tähän. orient="vertical" jakaa vaakasuunnassa.
        self.right_vertical_paned_window = tk.PanedWindow(self.right_content_area_for_paned, orient=tk.VERTICAL, sashwidth=6, sashrelief=tk.RAISED, bg=self.right_content_area_for_paned.cget("fg_color")[1])
        self.right_vertical_paned_window.grid(row=0, column=0, sticky="nsew")


        # TULOKSET (Oikean vertikaalisen PanedWindowin ylempi osa)
        self.results_outer_frame = ctk.CTkFrame(self.right_vertical_paned_window, height=400) # Anna lähtökorkeus
        self.right_vertical_paned_window.add(self.results_outer_frame, minsize=200, stretch="always")
        self.results_outer_frame.grid_rowconfigure(0, weight=0) # Otsikko
        self.results_outer_frame.grid_rowconfigure(1, weight=1) # Tulosgridi
        self.results_outer_frame.grid_columnconfigure(0, weight=1)

        # LOKIT (Oikean vertikaalisen PanedWindowin alempi osa)
        self.log_outer_frame = ctk.CTkFrame(self.right_vertical_paned_window, height=250) # Anna lähtökorkeus
        self.right_vertical_paned_window.add(self.log_outer_frame, minsize=150, stretch="always")
        self.log_outer_frame.grid_rowconfigure(0, weight=0) # Otsikko
        self.log_outer_frame.grid_rowconfigure(1, weight=1) # Loki-tabview
        self.log_outer_frame.grid_columnconfigure(0, weight=1)

        # Sijoitetaan vanhat _create_X_panel() -kutsut
        self._create_left_panel() # Tämä täyttää nyt self.left_frame:n
        self._create_results_panel() # Tämä täyttää self.results_outer_frame:n
        self._create_log_panel() # Tämä täyttää self.log_outer_frame:n

    def _create_left_panel(self):
        left_tabview = ctk.CTkTabview(self.left_frame)
        left_tabview.grid(row=0, column=0, sticky="nsew", padx=0, pady=0) # Tämä täyttää left_frame:n
        
        config_tab_base_frame = left_tabview.add("Konfiguraatio")
        config_tab_base_frame.grid_rowconfigure(0, weight=1) # Scrollable frame täyttää tämän
        config_tab_base_frame.grid_columnconfigure(0, weight=1)
        # --- Ajotila Välilehti ---
        run_mode_tab_frame = left_tabview.add("Ajotila")
        run_mode_tab_frame.grid_columnconfigure(0, weight=1) # Keskittää ja antaa leveyttä
        run_mode_tab_frame.grid_rowconfigure(0, weight=0)
        run_mode_tab_frame.grid_rowconfigure(1, weight=0)
        run_mode_tab_frame.grid_rowconfigure(2, weight=0) 

        scroll_content_frame = ctk.CTkScrollableFrame(config_tab_base_frame, label_text=None, fg_color="transparent")
        scroll_content_frame.grid(row=0, column=0, sticky="nsew") # Scrollable frame täyttää config_tab_base_frame:n

        parent_config_tab = scroll_content_frame

        # parent_config_tab:n (eli scroll_content_frame:n sisäisen kehyksen) grid-asetukset
        parent_config_tab.grid_columnconfigure(0, weight=1) # Kaikki lapset saavat leveyden
        parent_config_tab.grid_rowconfigure(0, weight=0)  # General settings
        parent_config_tab.grid_rowconfigure(1, weight=0)  # Test specific
        parent_config_tab.grid_rowconfigure(2, weight=1)  # Devices (tämä rivi venyy pystysuunnassa scrollattavan alueen sisällä)


        # --- YLEISET ASETUKSET (Rivi 0 parent_config_tab:ssa) ---
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
        baud_container.pack(fill="x", expand=False, padx=0, pady=5) # expand=False
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

        # --- TESTIKOHTAISET ASETUKSET (Rivi 1 parent_config_tab:ssa - Keskellä) ---
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

        test_order_container = ctk.CTkFrame(test_specific_actions_container)
        test_order_container.pack(fill="x", expand=False, padx=0, pady=5)
        ctk.CTkLabel(test_order_container, text="Testijärjestys", font=ctk.CTkFont(weight="bold")).pack(anchor="w", padx=10, pady=(5,2))
        test_order_frame = ctk.CTkFrame(test_order_container)
        test_order_frame.pack(fill="x", padx=5, pady=(0,5))
        ctk.CTkButton(test_order_frame, text="Muokkaa Testijärjestystä...", command=self.open_test_order_config).pack(fill="x", padx=5, pady=5)

        # --- LAITTEET (Rivi 2 parent_config_tab:ssa - Laajeneva) ---
        devices_outer_container = ctk.CTkFrame(parent_config_tab)
        devices_outer_container.grid(row=2, column=0, sticky="nsew", padx=10, pady=(5,10)) 

        # devices_outer_container:in sisäinen rakenne voi käyttää gridiä
        devices_outer_container.grid_rowconfigure(0, weight=0) # Otsikko
        devices_outer_container.grid_rowconfigure(1, weight=1) # devices_outer_frame (joka sisältää scrollattavan listan)
        devices_outer_container.grid_columnconfigure(0, weight=1)

        ctk.CTkLabel(devices_outer_container, text="Laitteet", font=ctk.CTkFont(weight="bold")).grid(row=0, column=0, sticky="w", padx=10, pady=(5,0))

        devices_outer_frame = ctk.CTkFrame(devices_outer_container)
        devices_outer_frame.grid(row=1, column=0, sticky="nsew", padx=0, pady=0)
        devices_outer_frame.grid_rowconfigure(0, weight=0) # dev_control_frame
        devices_outer_frame.grid_rowconfigure(1, weight=1) # scrollable_devices_frame
        devices_outer_frame.grid_columnconfigure(0, weight=1)

        dev_control_frame = ctk.CTkFrame(devices_outer_frame)
        dev_control_frame.grid(row=0, column=0, sticky="ew", padx=5, pady=(5,5))
        ctk.CTkButton(dev_control_frame, text="Lisää Laite", command=self.add_device).pack(side="left", padx=5) # Pack ok tässä sisemmällä tasolla
        ctk.CTkButton(dev_control_frame, text="Poista Viimeinen", command=self.remove_device).pack(side="left", padx=5) # Pack ok

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

    def _create_device_frames_widgets(self):
        for widget in self.scrollable_devices_frame.winfo_children(): widget.destroy()
        self.device_frames.clear(); self.device_names_vars.clear(); self.flash_ports.clear()
        self.monitor_ports.clear(); self.modbus_ports.clear(); self.modbus_slave_ids.clear()
        self.test_selection_vars.clear()
        ports = self.get_serial_ports()

        for i in range(1, self.current_device_count + 1):
            device_state = self.device_state.get(i, self._create_default_device_state(i))
            self.device_state[i] = device_state

            dev_container = ctk.CTkFrame(self.scrollable_devices_frame) 
            dev_container.pack(fill="x", expand=True, padx=0, pady=(0, 5)) # expand=True TÄRKEÄ
            self.device_frames[i] = dev_container
            
            dev_title_frame = ctk.CTkFrame(dev_container, fg_color="transparent")
            dev_title_frame.pack(fill="x", padx=5, pady=(2,1))

            dev_title_label = ctk.CTkLabel(dev_title_frame, text=device_state['config']['name'], font=ctk.CTkFont(weight="bold"))
            dev_title_label.pack(side="left", anchor="w", padx=(5,0))

            dev_content_frame = ctk.CTkFrame(dev_container)
            dev_content_frame.pack(fill="x", expand=True, padx=5, pady=(0,2)) # expand=True TÄRKEÄ

            name_frame = ctk.CTkFrame(dev_content_frame)
            name_frame.pack(fill="x", padx=5, pady=1)
            ctk.CTkLabel(name_frame, text="Nimi:", width=50).pack(side="left", padx=(0,2))
            name_var = ctk.StringVar(value=device_state['config']['name'])
            self.device_names_vars[i] = name_var
            name_entry = ctk.CTkEntry(name_frame, textvariable=name_var)
            name_entry.pack(side="left", fill="x", expand=True, padx=2)
            name_var.trace_add("write", lambda *args, idx=i, lbl=dev_title_label: self._update_single_device_name_in_ui(idx, lbl))

            ports_frame = ctk.CTkFrame(dev_content_frame)
            ports_frame.pack(fill="x", padx=5, pady=1)
            ports_frame.grid_columnconfigure(1, weight=1)
            ports_frame.grid_columnconfigure(3, weight=1)
           
            ctk.CTkLabel(ports_frame, text="Flash:", width=50).grid(row=0, column=0, sticky="w", padx=(0,2), pady=1)
            flash_var = ctk.StringVar(value=device_state['config']['flash_port']); self.flash_ports[i] = flash_var
            ctk.CTkComboBox(ports_frame, variable=flash_var, values=[""] + ports, width=100, state='readonly').grid(row=0, column=1, sticky="ew", padx=2, pady=1)
            ctk.CTkLabel(ports_frame, text="Monitor:", width=50).grid(row=1, column=0, sticky="w", padx=(0,2), pady=1)
            monitor_var = ctk.StringVar(value=device_state['config']['monitor_port']); self.monitor_ports[i] = monitor_var
            ctk.CTkComboBox(ports_frame, variable=monitor_var, values=[""] + ports, width=100, state='readonly').grid(row=1, column=1, sticky="ew", padx=2, pady=1)
            ctk.CTkLabel(ports_frame, text="Modbus:", width=50).grid(row=0, column=2, sticky="w", padx=(10,2), pady=1)
            modbus_var = ctk.StringVar(value=device_state['config']['modbus_port']); self.modbus_ports[i] = modbus_var
            ctk.CTkComboBox(ports_frame, variable=modbus_var, values=[""] + ports, width=100, state='readonly').grid(row=0, column=3, sticky="ew", padx=2, pady=1)
            ctk.CTkLabel(ports_frame, text="SlaveID:", width=50).grid(row=1, column=2, sticky="w", padx=(10,2), pady=1)
            slave_var = ctk.StringVar(value=str(device_state['config']['modbus_slave_id'])); self.modbus_slave_ids[i] = slave_var
            ctk.CTkEntry(ports_frame, textvariable=slave_var, width=50).grid(row=1, column=3, sticky="w", padx=2, pady=1)

            tests_frame_container = ctk.CTkFrame(dev_content_frame)
            tests_frame_container.pack(fill="x", padx=5, pady=(2,1))

            ctk.CTkLabel(tests_frame_container, text="Valitut testivaiheet:").pack(anchor="w", padx=5, pady=(0,1))
            
            tests_scroll_frame = ctk.CTkScrollableFrame(tests_frame_container, label_text=None, height=75) 
            tests_scroll_frame.pack(fill="x", expand=False, pady=1, ipady=2)

            for test_step_config in self.test_order:
                test_step_id = test_step_config['id']
                test_type = test_step_config['type']
                display_name = test_step_config.get('name', f"{AVAILABLE_TEST_TYPES.get(test_type, test_type.capitalize())} (ID: {test_step_id[:5]})")
                key = (i, test_step_id)
                if test_step_id not in device_state['config']['tests_enabled']:
                    device_state['config']['tests_enabled'][test_step_id] = False
                var = ctk.BooleanVar(value=device_state['config']['tests_enabled'].get(test_step_id, False))
                self.test_selection_vars[key] = var
                cb_frame = ctk.CTkFrame(tests_scroll_frame)
                cb_frame.pack(fill="x", pady=0)
                cb = ctk.CTkCheckBox(cb_frame, text=display_name, variable=var)
                if (test_type == 'daq' and not NIDAQMX_AVAILABLE) or \
                   (test_type == 'modbus' and not PYMODBUS_AVAILABLE):
                    cb.configure(state="disabled"); var.set(False)
                cb.pack(side="left", padx=5, pady=0)



    def _create_results_panel(self):
        parent = self.results_outer_frame # results_outer_frame on jo konfiguroitu venymään
        results_title_frame = ctk.CTkFrame(parent);
        # MUUTOS: Käytä gridiä parentin (results_outer_frame) sisällä
        results_title_frame.grid(row=0, column=0, sticky="ew", padx=5, pady=(5,0))
        ctk.CTkLabel(results_title_frame, text="Testitulokset", font=ctk.CTkFont(weight="bold")).pack(anchor="w", padx=5) # Pack on ok tässä

        self.test_results_ui = TestResultsUI(parent, self)
        self.test_results_ui.grid(row=1, column=0, sticky="nsew", padx=5, pady=(0,5)) # Tämä venyy

    def _create_log_panel(self):
        parent = self.log_outer_frame # log_outer_frame on jo konfiguroitu venymään
        log_container_frame = ctk.CTkFrame(parent) # Nimeä selkeämmin, erotuksena log_containerista workerissa
        # MUUTOS: Käytä gridiä parentin (log_outer_frame) sisällä
        log_container_frame.grid(row=1, column=0, sticky="nsew", padx=5, pady=(0,5)) # HUOM: row=1, jos otsikko on row=0
        # Varmista, että log_container_frame:n sisäinen grid on konfiguroitu
        log_container_frame.grid_rowconfigure(0, weight=0) # Otsikolle
        log_container_frame.grid_rowconfigure(1, weight=1) # Tabviewille
        log_container_frame.grid_columnconfigure(0, weight=1)

        ctk.CTkLabel(log_container_frame, text="Loki", font=ctk.CTkFont(weight="bold")).grid(row=0, column=0, sticky="w", padx=5, pady=(5,0)) # Grid täällä
        self.log_tabview = ctk.CTkTabview(log_container_frame)
        self.log_tabview.grid(row=1, column=0, sticky="nsew", padx=0, pady=0) # Grid ja sticky
        self.root.after(150, self._update_log_tabs)

    #ALKUPERÄINEN
    def _update_log_tabs(self):

        if not hasattr(self, 'log_tabview') or not self.log_tabview.winfo_exists(): # Turvallisempi tarkistus
            print("Varoitus: _update_log_tabs kutsuttu, mutta log_tabview ei ole valmis.")
            return
            
        active_tab_name = None
        try:
            if self.log_tabview.get(): 
                 active_tab_name = self.log_tabview.get()
        except Exception: 
            pass

        current_tab_names = list(self.log_tabview._tab_dict.keys())
        for tab_name in current_tab_names:
            try: self.log_tabview.delete(tab_name)
            except Exception as e: print(f"Varoitus: Ei voitu poistaa logitabiä '{tab_name}': {e}")
        self.log_text_widgets.clear()

        new_tab_names = []
        for i in range(self.current_device_count):
            device_idx = i + 1
            dev_name = self.device_state.get(device_idx, {}).get('config',{}).get('name', f"Laite {device_idx}")
            unique_name = dev_name
            count = 1
            while unique_name in new_tab_names: 
                unique_name = f"{dev_name}_{count}"
                count += 1
            new_tab_names.append(unique_name)

            try:
                self.log_tabview.add(unique_name)
                tab_frame = self.log_tabview.tab(unique_name)
                if tab_frame:
                    # Konfiguroi tab_frame:n sisäinen grid, jotta CTkTextbox laajenee
                    tab_frame.grid_rowconfigure(0, weight=1)
                    tab_frame.grid_columnconfigure(0, weight=1)

                    text_widget = ctk.CTkTextbox(tab_frame, wrap="word", height=10, font=("Courier New", 9))
                    text_widget.grid(row=0, column=0, sticky="nsew", padx=0, pady=0) # Käytä gridiä ja stickyä
                    text_widget.configure(state='disabled')
                    self.log_text_widgets[device_idx] = text_widget
                else:
                    print(f"Virhe: Ei saatu luotua tab_framea nimelle '{unique_name}'")
            except Exception as e:
                print(f"Virhe lisättäessä/konfiguroitaessa logitabiä '{unique_name}': {e}")

        if active_tab_name and active_tab_name in self.log_tabview._name_list:
            try: self.log_tabview.set(active_tab_name)
            except Exception as e: print(f"Varoitus: Ei voitu palauttaa aktiivista logitabiä '{active_tab_name}': {e}")
        elif new_tab_names:
            try: self.log_tabview.set(new_tab_names[0])
            except Exception as e: print(f"Varoitus: Ei voitu asettaa ensimmäistä logitabiä '{new_tab_names[0]}': {e}")

    def _update_config_from_ui_for_save_or_start(self):
        for i in range(1, self.current_device_count + 1):
            if i in self.device_state:
                config = self.device_state[i]['config']
                if i in self.device_names_vars: config['name'] = self.device_names_vars[i].get()
                if i in self.flash_ports: config['flash_port'] = self.flash_ports[i].get()
                if i in self.monitor_ports: config['monitor_port'] = self.monitor_ports[i].get()
                if i in self.modbus_ports: config['modbus_port'] = self.modbus_ports[i].get()
                if i in self.modbus_slave_ids: config['modbus_slave_id'] = self.modbus_slave_ids[i].get()
                
                current_tests_enabled = {}
                for step_cfg in self.test_order:
                    step_id = step_cfg['id']; selection_key = (i, step_id)
                    if selection_key in self.test_selection_vars:
                        current_tests_enabled[step_id] = self.test_selection_vars[selection_key].get()
                    else: current_tests_enabled[step_id] = False 
                config['tests_enabled'] = current_tests_enabled
    
    def _update_results_ui_layout(self): 
         if hasattr(self, 'test_results_ui'):
             device_names = {idx: state['config']['name'] for idx, state in self.device_state.items() if idx <= self.current_device_count}
             self.test_results_ui.create_results_grid(self.current_device_count, device_names, self.test_order)

    def _update_single_device_name_in_ui(self, device_idx: int, title_label_widget: ctk.CTkLabel):
        if device_idx in self.device_state and device_idx in self.device_names_vars:
            new_name = self.device_names_vars[device_idx].get()
            if not new_name: new_name = f"Laite {device_idx}"
            
            self.device_state[device_idx]['config']['name'] = new_name
            if isinstance(title_label_widget, ctk.CTkLabel): # Make sure it's the correct widget
                title_label_widget.configure(text=new_name)
            
            if hasattr(self, 'test_results_ui'):
                device_names_map = {idx: state['config']['name'] for idx, state in self.device_state.items() if idx <= self.current_device_count}
                self.test_results_ui.update_device_names(device_names_map)
            self._update_log_tabs()


    def _update_device_names_in_ui(self): 
         device_names_map = {}
         for i in range(1, self.current_device_count + 1):
            name = self.device_names_vars.get(i, ctk.StringVar(value=f"Laite {i}")).get()
            if not name: name = f"Laite {i}"
            device_names_map[i] = name
            
            if i in self.device_frames:
                 try:
                    # The device title label is now inside dev_title_frame, which is the first child of dev_container
                    dev_title_frame = self.device_frames[i].winfo_children()[0]
                    # The label itself is the first child of dev_title_frame
                    title_label = dev_title_frame.winfo_children()[0]
                    if isinstance(title_label, ctk.CTkLabel): title_label.configure(text=name)
                 except IndexError:
                    print(f"Warning: Could not find title label for device {i} during general update.")

            if i in self.device_state: self.device_state[i]['config']['name'] = name
         if hasattr(self, 'test_results_ui'): self.test_results_ui.update_device_names(device_names_map)
         self._update_log_tabs() 

    def _browse_file(self, string_var: ctk.StringVar): 
        filename = filedialog.askopenfilename(parent=self.root);_=[string_var.set(filename)] if filename else None


    def open_wait_info_config(self):
       selected_step_id = self._select_test_step_for_config('wait_info', "Odotus/Info Asetukset")
       if not selected_step_id:
           return

       current_wait_info_settings = self.step_specific_settings.get(selected_step_id, self._get_default_settings_for_step_type('wait_info'))
       config_window = WaitInfoConfigWindow(self.root, current_wait_info_settings) 
       updated_settings = config_window.get_settings()
       if updated_settings is not None:
           self.step_specific_settings[selected_step_id] = updated_settings
           step_name = next((s['name'] for s in self.test_order if s['id'] == selected_step_id), "Odotus/Info")
           print(f"'{step_name}' asetukset päivitetty.")

    def _select_test_step_for_config_extended(self,
                                            target_config_type: str, # Esim. 'daq', 'serial' (MITÄ ollaan muokkaamassa)
                                            window_title_prefix: str
                                            ) -> Optional[Tuple[str, str, Optional[str]]]:
        # Palauttaa: (valitun_vaiheen_id, target_config_type, valitun_komposiittityypin_avain_jos_relevantti)

        # 1. Etsi kaikki vaiheet, jotka SISÄLTÄVÄT `target_config_type`
        relevant_steps_for_target = [] # Lista (vaiheen_id, vaiheen_tyyppi, vaiheen_nimi)
        # Itsenäiset vaiheet
        for step in self.test_order:
            if step['type'] == target_config_type:
                relevant_steps_for_target.append((step['id'], step['type'], step['name'], None)) # None = ei komposiitti

        # Komposiittivaiheet, jotka sisältävät target_config_typen
        # Tässä pitäisi olla fiksumpi tapa tunnistaa, mitkä komposiitit sisältävät mitä.
        # Oletetaan nyt, että tiedämme sen etukäteen.
        composite_map = {
            'daq': [('daq_and_serial', "'DAQ ja Sarjatesti'"), ('daq_and_modbus', "'DAQ ja Modbus'")],
            'serial': [('daq_and_serial', "'DAQ ja Sarjatesti'")],
            'modbus': [('daq_and_modbus', "'DAQ ja Modbus'")]
        }
        relevant_composite_types_for_target = composite_map.get(target_config_type, [])

        for step in self.test_order:
            for comp_type_key, _ in relevant_composite_types_for_target:
                if step['type'] == comp_type_key:
                    # Lisätään vain kerran per komposiittivaihe, mutta merkitään, että se on komposiitti
                    # ja mikä sen komposiittityyppi on.
                    if not any(s[0] == step['id'] and s[3] == comp_type_key for s in relevant_steps_for_target):
                         relevant_steps_for_target.append((step['id'], step['type'], step['name'], comp_type_key))


        if not relevant_steps_for_target:
            messagebox.showinfo("Ei vaiheita", f"Ei '{target_config_type}'-tyyppisiä tai sitä sisältäviä vaiheita löydy.", parent=self.root)
            return None

        # Jos vain yksi relevantti vaihe löytyi KAIKISTA (itsenäinen TAI komposiitti, joka sisältää targetin)
        if len(relevant_steps_for_target) == 1:
            step_id, original_step_type, _, composite_parent_type = relevant_steps_for_target[0]
            # Jos se on komposiitti, palautetaan sen tyyppi, muuten None
            return step_id, target_config_type, (original_step_type if original_step_type != target_config_type else None)


        # Muuten, näytä dialogi, jossa käyttäjä voi valita
        choices_for_dialog_display_to_data = {} # {näyttönimi: (id, original_type, composite_parent_type)}
        for step_id, original_type, step_name, composite_parent_type_key in relevant_steps_for_target:
            display_name = f"{step_name} (Tyyppi: {AVAILABLE_TEST_TYPES.get(original_type, original_type)})"
            if composite_parent_type_key:
                 display_name += f" - [Osa: {target_config_type.upper()}]"
            count = 1
            base_display_name = display_name
            while display_name in choices_for_dialog_display_to_data: # Käytä uutta nimeä täällä
                display_name = f"{base_display_name} ({count})"
                count += 1
            choices_for_dialog_display_to_data[display_name] = (step_id, original_type, composite_parent_type_key)


        dialog_prompt = f"Valitse '{window_title_prefix}'-vaihe tai sen osa, jonka asetuksia haluat muokata:"
        selection_dialog = SelectStepDialog(self.root, f"{window_title_prefix} Asetukset", dialog_prompt, choices_for_dialog_display_to_data)
        selected_display_key = selection_dialog.get_selected_key() # Käytä uutta metodinimeä

        if selected_display_key and selected_display_key in choices_for_dialog_display_to_data:
            step_id, original_step_type, composite_parent_type = choices_for_dialog_display_to_data[selected_display_key]
            return step_id, target_config_type, (original_step_type if original_step_type != target_config_type else None)
        return None

    def open_daq_config(self):
        if not NIDAQMX_AVAILABLE: return
        selection = self._select_test_step_for_config_extended('daq', "DAQ Testin")
        if not selection: return

        step_id_to_configure, target_config_key, composite_parent_type = selection
        # target_config_key on aina 'daq' tässä tapauksessa

        current_settings_for_daq_part = {}
        if composite_parent_type: # Olemme muokkaamassa DAQ-osaa komposiittivaiheesta
            composite_settings = self.step_specific_settings.get(step_id_to_configure, {})
            current_settings_for_daq_part = composite_settings.get('daq_settings', self._get_default_daq_settings())
        else: # Itsenäinen DAQ-vaihe
            current_settings_for_daq_part = self.step_specific_settings.get(step_id_to_configure, self._get_default_daq_settings())

        config_window = DAQConfigWindow(self.root, current_settings_for_daq_part)
        updated_settings = config_window.get_settings()

        if updated_settings is not None:
            step_name_for_log = next((s['name'] for s in self.test_order if s['id'] == step_id_to_configure), "DAQ-testi")
            if composite_parent_type:
                self.step_specific_settings.setdefault(step_id_to_configure, {}) # Varmista, että komposiitin pääavain on olemassa
                self.step_specific_settings[step_id_to_configure]['daq_settings'] = updated_settings
                print(f"'{step_name_for_log}' (Tyyppi: {composite_parent_type}) DAQ-osion asetukset päivitetty.")
            else:
                self.step_specific_settings[step_id_to_configure] = updated_settings
                print(f"Itsenäisen DAQ-vaiheen '{step_name_for_log}' asetukset päivitetty.")

    def open_serial_config(self):
        selection = self._select_test_step_for_config_extended('serial', "Sarjatestin")
        if not selection: return

        step_id_to_configure, target_config_key, composite_parent_type = selection

        current_settings_for_serial_part = {}
        if composite_parent_type:
            composite_settings = self.step_specific_settings.get(step_id_to_configure, {})
            current_settings_for_serial_part = composite_settings.get('serial_settings', self._get_default_serial_settings())
        else:
            current_settings_for_serial_part = self.step_specific_settings.get(step_id_to_configure, self._get_default_serial_settings())

        config_window = SerialConfigWindow(self.root, current_settings_for_serial_part)
        updated_settings = config_window.get_settings()

        if updated_settings is not None:
            step_name_for_log = next((s['name'] for s in self.test_order if s['id'] == step_id_to_configure), "Sarjatesti")
            if composite_parent_type:
                self.step_specific_settings.setdefault(step_id_to_configure, {})
                self.step_specific_settings[step_id_to_configure]['serial_settings'] = updated_settings
                print(f"'{step_name_for_log}' (Tyyppi: {composite_parent_type}) Sarjatesti-osion asetukset päivitetty.")

            else:
                self.step_specific_settings[step_id_to_configure] = updated_settings
                print(f"Itsenäisen Sarjatestin '{step_name_for_log}' asetukset päivitetty.")


    def open_modbus_config(self):
        if not PYMODBUS_AVAILABLE: return
        selection = self._select_test_step_for_config_extended('modbus', "Modbus Testin")
        if not selection: return

        step_id_to_configure, target_config_key, composite_parent_type = selection

        current_settings_for_modbus_part = [] # Modbus käyttää listaa (sekvenssi)
        if composite_parent_type:
            composite_settings = self.step_specific_settings.get(step_id_to_configure, {})
            current_settings_for_modbus_part = composite_settings.get('modbus_settings', self._get_default_modbus_sequence())
        else:
            current_settings_for_modbus_part = self.step_specific_settings.get(step_id_to_configure, self._get_default_modbus_sequence())

        config_window = ModbusConfigWindow(self.root, current_settings_for_modbus_part)
        updated_sequence = config_window.get_sequence()

        if updated_sequence is not None:
            step_name_for_log = next((s['name'] for s in self.test_order if s['id'] == step_id_to_configure), "Modbus-testi")
            if composite_parent_type:
                self.step_specific_settings.setdefault(step_id_to_configure, {})
                self.step_specific_settings[step_id_to_configure]['modbus_settings'] = updated_sequence
                print(f"'{step_name_for_log}' (Tyyppi: {composite_parent_type}) Modbus-osion asetukset päivitetty.")
            else:
                self.step_specific_settings[step_id_to_configure] = updated_sequence
                print(f"Itsenäisen Modbus-vaiheen '{step_name_for_log}' asetukset päivitetty.")

    def open_test_order_config(self):
        # Varmista, että test_orderissa on jo oletus retry-avaimet ennen ikkunan avaamista
        self._ensure_test_order_attributes() 
        config_window = TestOrderConfigWindow(self.root, self.test_order, self.default_retry_delay_s)
        updated_order = config_window.get_test_order()
        if updated_order is not None:
            self.test_order = updated_order
            # _ensure_step_specific_settings-metodia ei välttämättä tarvitse kutsua tässä,
            # jos TestOrderConfigWindow jo varmistaa kaikkien avainten olemassaolon.
            # Mutta _ensure_test_order_attributes on hyvä varmistus.
            self._ensure_test_order_attributes() 
            print("Testijärjestys ja retry-asetukset päivitetty.")
            self._initialize_device_states() 
            self._create_device_frames_widgets() 
            self._update_results_ui_layout() 
            messagebox.showinfo("Testijärjestys", "Testijärjestys ja retry-asetukset päivitetty.", parent=self.root)
   
    def add_device(self): 
        if self.current_device_count >= MAX_DEVICES: messagebox.showwarning("Maksimi", f"Max laitteita ({MAX_DEVICES}) saavutettu.",parent=self.root); return
        self.current_device_count += 1
        self._ensure_step_specific_settings() 
        if self.current_device_count not in self.device_state: self.device_state[self.current_device_count] = self._create_default_device_state(self.current_device_count)
        self._create_device_frames_widgets(); self._update_results_ui_layout(); self._update_log_tabs()
        if hasattr(self,'scrollable_devices_frame') and self.scrollable_devices_frame._parent_canvas: 
            self.root.after(50, lambda: self.scrollable_devices_frame._parent_canvas.yview_moveto(1.0))


    def remove_device(self): 
        if self.current_device_count <= 0 : return 
        if self.current_device_count == 1 and len(self.device_state) == 1: 
            messagebox.showwarning("Minimi", "Vähintään yksi laite vaaditaan.",parent=self.root); return
            
        idx_to_remove = self.current_device_count
        if idx_to_remove in self.device_state: del self.device_state[idx_to_remove]
        if idx_to_remove in self.device_frames: self.device_frames[idx_to_remove].destroy(); del self.device_frames[idx_to_remove]
        
        keys_to_del = [key for key in self.test_selection_vars if key[0] == idx_to_remove]
        for key in keys_to_del: del self.test_selection_vars[key]
        
        self.current_device_count -= 1
        if self.current_device_count < 0: self.current_device_count = 0 

        self._update_results_ui_layout(); self._update_log_tabs()
    
    def clear_all_ports(self): 
        for i in range(1, self.current_device_count + 1):
            if i in self.flash_ports: self.flash_ports[i].set("")
            if i in self.monitor_ports: self.monitor_ports[i].set("")
            if i in self.modbus_ports: self.modbus_ports[i].set("")
            if i in self.device_state:
                self.device_state[i]['config']['flash_port'] = ""
                self.device_state[i]['config']['monitor_port'] = ""
                self.device_state[i]['config']['modbus_port'] = ""
        messagebox.showinfo("Portit", "Kaikki COM-valinnat tyhjennetty.")

    def _cache_serial_ports(self):
        self.cached_ports = list_serial_ports()
        self.last_port_scan_time = time.time()
        return self.cached_ports

    def get_serial_ports(self, force_refresh=False):
        current_time = time.time()
        if not hasattr(self, 'cached_ports') or not self.cached_ports or \
        not hasattr(self, 'last_port_scan_time') or \
        current_time - self.last_port_scan_time > 5 or force_refresh:
            return self._cache_serial_ports()
        return self.cached_ports

    def _refresh_single_device_ports_ui(self, device_idx_to_refresh: int):
        """Refreshes port comboboxes for a single device."""
        ports = self.get_serial_ports(force_refresh=True) 
        ports_for_combobox = [""] + ports

        if device_idx_to_refresh in self.device_frames:
            # Navigate to the ports_frame for the specific device
            # dev_container -> dev_content_frame -> ports_frame
            try:
                dev_container = self.device_frames[device_idx_to_refresh]
                dev_content_frame = dev_container.winfo_children()[2] # dev_title_frame, dev_content_frame
                ports_frame = dev_content_frame.winfo_children()[1] # name_frame, ports_frame, tests_frame_container
            except IndexError:
                print(f"Error: Could not find ports_frame for device {device_idx_to_refresh} during single refresh.")
                return

            comboboxes_in_frame = []
            for widget in ports_frame.winfo_children():
                if isinstance(widget, ctk.CTkComboBox):
                    comboboxes_in_frame.append(widget)
            
            for cb in comboboxes_in_frame:
                current_value = cb.get()
                cb.configure(values=ports_for_combobox)
                if current_value in ports_for_combobox:
                    cb.set(current_value)
                else:
                    cb.set(ports_for_combobox[0])
            print(f"Ports refreshed for device {device_idx_to_refresh}.")


    def refresh_ports(self):
        """Päivitä saatavilla olevat sarjaportit kaikissa porttien valintalaatikoissa."""
        ports = self.get_serial_ports(force_refresh=True) 
        ports_for_combobox = [""] + ports  
        
        for i in range(1, self.current_device_count + 1):
            if i in self.device_frames:
                try:
                    # dev_container is self.device_frames[i]
                    # First child of dev_container is dev_title_frame
                    # Second child of dev_container is dev_content_frame
                    dev_content_frame = self.device_frames[i].winfo_children()[1] # dev_title_frame, dev_content_frame
                    # First child of dev_content_frame is name_frame
                    # Second child of dev_content_frame is ports_frame
                    ports_frame = dev_content_frame.winfo_children()[1]
                except IndexError:
                    print(f"Error finding ports_frame for device {i} during general refresh.")
                    continue # Skip this device if structure is unexpected
                
                comboboxes_in_frame = []
                for widget in ports_frame.winfo_children():
                    if isinstance(widget, ctk.CTkComboBox):
                        comboboxes_in_frame.append(widget)
                
                for cb in comboboxes_in_frame:
                    current_value = cb.get()
                    cb.configure(values=ports_for_combobox)
                    if current_value in ports_for_combobox:
                        cb.set(current_value)
                    else:
                        cb.set(ports_for_combobox[0]) 
        
        if hasattr(self, 'root'): 
            print("All device port lists refreshed.")
            # Consider a brief status message via a new status bar or temporary label if desired.

    def start_tests(self):
        print("Starting tests...")
        self._ensure_test_order_attributes() # Tärkeää ennen mitään muuta
        self._update_device_names_in_ui()
        self._update_config_from_ui_for_save_or_start()

        self.device_log_buffer.clear()
        timestamp_file = time.strftime("%Y%m%d_%H%M%S")
        log_dir = "test_logs"; os.makedirs(log_dir, exist_ok=True)
        self.current_test_run_log_filename = os.path.join(log_dir, f"testiajo_{timestamp_file}.csv")
        self._log_message(0, f"Aloitetaan testiajo. Lokit: {self.current_test_run_log_filename}")

        self.shared_flash_queues.clear()
        flash_sequentially = self.flash_in_sequence_var.get()
        
        files = {'bootloader': self.bootloader_path.get(), 'partitions': self.partitions_path.get(), 'app': self.app_path.get()}
        flash_needed_overall = any(
            step_cfg['type'] == 'flash' and self.device_state[i]['config']['tests_enabled'].get(step_cfg['id'], False)
            for i in range(1, self.current_device_count + 1) if i in self.device_state
            for step_cfg in self.test_order
        )
        if flash_needed_overall and not all(f and os.path.exists(f) for f in files.values()):
             missing_files = [name for name,path in files.items() if not path or not os.path.exists(path)]
             messagebox.showerror("Tiedostot", f"Flashaus valittu, mutta puuttuvat tiedostot: {', '.join(missing_files)}.", parent=self.root); return

        devices_to_run_initial = []; port_usage = {}; flash_sharers = {}; validation_errors = []
        for i in range(1, self.current_device_count + 1):
            if i not in self.device_state: continue
            state = self.device_state[i]; config = state['config']
            if not any(config['tests_enabled'].values()):
                 state['runtime']['last_status_msg'] = "Ei valittu"; state['runtime']['final_result'] = "SKIPPED"
                 self._update_ui_for_final_result(i); continue
            devices_to_run_initial.append(i)
            # ... (porttikonfliktien validointi kuten ennen) ...

        devices_to_run = [idx for idx in devices_to_run_initial if not any(f"Laite {idx}" in err for err in validation_errors)]
        if validation_errors:
             messagebox.showerror("Asetusvirhe", "Korjaa asetukset:\n\n- " + "\n- ".join(sorted(list(set(validation_errors)))), parent=self.root); return
        if not devices_to_run:
            messagebox.showinfo("Ei testejä", "Ei valittuja laitteita tai kaikilla virheitä.", parent=self.root); return

        self.test_results_ui.reset_results(self.test_order)
        self.stop_events.clear(); self.active_threads.clear()
        
        if flash_sequentially: self.shared_flash_queues = {port: dev_list for port, dev_list in flash_sharers.items() if dev_list}
        else: self.shared_flash_queues.clear()
        
        for device_idx in devices_to_run:
            state = self.device_state[device_idx]; runtime = state['runtime']
            runtime['steps_status'] = {step['id']: None for step in self.test_order}
            runtime['step_attempts'] = {step['id']: 0 for step in self.test_order} # Nollaa yritykset!
            runtime.update({'sequence_running': True, 'current_stage_test_step_id': None, 
                            'final_result': None, 'last_status_msg': "Alustetaan...", 
                            'start_time': time.time(), 'end_time': None})
            state['busy_flags'] = {k: False for k in state['busy_flags']}
            if device_idx in self.log_text_widgets:
                 lw = self.log_text_widgets[device_idx]; lw.configure(state='normal'); lw.delete("1.0", "end"); lw.configure(state='disabled')
            self.stop_events[device_idx] = threading.Event()
            self._log_message(device_idx, f"=== Testisekvenssi aloitettu: {state['config']['name']} ===")
            self._start_next_test_step(device_idx)

    def _start_next_test_step(self, device_idx: int):
        if device_idx not in self.device_state or device_idx not in self.stop_events: return
        state = self.device_state[device_idx]; runtime = state['runtime']; config = state['config']
        stop_event = self.stop_events[device_idx]

        if stop_event.is_set():
            if runtime['sequence_running']:
                 runtime.update({'last_status_msg': "Pysäytetty", 'final_result': "STOPPED", 'sequence_running': False, 'end_time': time.time()})
                 self._log_message(device_idx, "--- Testisekvenssi pysäytetty ---")
                 self._update_ui_for_final_result(device_idx)
            return
        if not runtime['sequence_running']: return

        next_test_step_id_to_run = None
        for step_cfg in self.test_order:
            step_id = step_cfg['id']
            if config['tests_enabled'].get(step_id, False) and runtime['steps_status'].get(step_id) is None:
                next_test_step_id_to_run = step_id
                break
        
        if next_test_step_id_to_run is None: 
            runtime['sequence_running'] = False; runtime['end_time'] = time.time()
            all_passed = all(runtime['steps_status'].get(s['id']) is True for s in self.test_order if config['tests_enabled'].get(s['id']))
            any_failed = any(runtime['steps_status'].get(s['id']) is False for s in self.test_order if config['tests_enabled'].get(s['id']))
            runtime['final_result'] = "FAIL" if any_failed else ("PASS" if all_passed else "SKIPPED") # SKIPPED jos ei yhtään ajettu/päässyt loppuun
            runtime['last_status_msg'] = f"Valmis ({runtime['final_result']})"
            duration = runtime['end_time'] - runtime['start_time'] if runtime['start_time'] else 0
            self._log_message(device_idx, f"=== Testisekvenssi valmis ({runtime['final_result']}). Kesto: {duration:.2f}s ===")
            self._update_ui_for_final_result(device_idx)
            return
        
        # Käynnistä löydetty seuraava vaihe
        self._start_specific_test_step(device_idx, next_test_step_id_to_run, is_retry=False)

    def _start_specific_test_step(self, device_idx: int, target_step_id: str, is_retry: bool = False):
        if device_idx not in self.device_state or device_idx not in self.stop_events:
            print(f"DEBUG: _start_specific_test_step: Laite {device_idx} ei ole valmis.")
            return
            
        state = self.device_state[device_idx]; runtime = state['runtime']; config = state['config']
        stop_event = self.stop_events[device_idx]

        if stop_event.is_set() or not runtime['sequence_running']:
            if runtime['sequence_running']: # Jos oli käynnissä mutta nyt stop_event
                 runtime.update({'last_status_msg': "Pysäytetty", 'final_result': "STOPPED", 'sequence_running': False, 'end_time': time.time()})
                 self._log_message(device_idx, f"--- Testisekvenssi pysäytetty (vaiheen {target_step_id} aloituksen aikana) ---")
                 self._update_ui_for_final_result(device_idx)
            else:
                self._log_message(device_idx, f"Testisekvenssi ei ole käynnissä laitteelle {device_idx}, ei aloiteta vaihetta {target_step_id}.")
            return

        target_step_config = next((s_cfg for s_cfg in self.test_order if s_cfg['id'] == target_step_id), None)

        if target_step_config is None:
            self._log_message(device_idx, f"Virhe: Ei löydy konfiguraatiota kohdevaiheelle ID {target_step_id}. Siirrytään seuraavaan.", error=True)
            self.root.after(10, lambda idx=device_idx: self._start_next_test_step(idx))
            return
        
        if not config['tests_enabled'].get(target_step_id):
            self._log_message(device_idx, f"Vaihe {target_step_config.get('name')} (ID: {target_step_id}) ei ole valittu ajettavaksi. Ohitetaan ja siirrytään seuraavaan.", error=False) # Ei virhe, normaali ohitus
            runtime['steps_status'][target_step_id] = None # Merkitään None (skip)
            self.test_results_ui.update_test_result(device_idx, target_step_id, None, running=False) # Näytä skip-väri
            self.root.after(10, lambda idx=device_idx: self._start_next_test_step(idx))
            return


        current_test_type = target_step_config['type']
        current_test_display_name = target_step_config.get('name', AVAILABLE_TEST_TYPES.get(current_test_type, current_test_type))
        
        attempt_count = runtime['step_attempts'].get(target_step_id, 0) # Nykyinen yritysmäärä (0-indeksoitu ennen tätä yritystä)
        
        if is_retry:
            self._log_message(device_idx, f"--- Uudelleenyritys vaiheelle: {current_test_display_name} (Yritys {attempt_count + 1}) ---")
        else: # Ensimmäinen yritys tälle vaiheelle tässä sekvenssin ajossa
             runtime['step_attempts'][target_step_id] = 0 # Varmista nollaus ensimmäisellä kerralla
             self._log_message(device_idx, f"--- Aloitetaan vaihe: {current_test_display_name} ---")

        runtime['current_stage_test_step_id'] = target_step_id
        runtime['last_status_msg'] = f"Käynnistetään {current_test_display_name}..."
        self.test_results_ui.update_test_result(device_idx, target_step_id, None, running=True) # Näytä keltainen

        # Puhdista vanha säie, jos sellainen on aktiivinen tälle vaiheelle (epätodennäköistä, mutta turvallisuuden vuoksi)
        old_thread_key = (device_idx, target_step_id)
        if old_thread_key in self.active_threads:
            self._log_message(device_idx, f"Varoitus: Poistetaan vanha aktiivinen säie vaiheelle {target_step_id} ennen uutta yritystä.", error=True)
            # Tässä voisi yrittää liittyä vanhaan säikeeseen tai lähettää sille stop-eventin,
            # mutta yksinkertaisuuden vuoksi poistetaan vain viittaus. Workerien pitäisi tarkistaa stop_event.
            del self.active_threads[old_thread_key]

        thread = None; target_func = None; args = []; kwargs = {}
        try:
            step_settings = self.step_specific_settings.get(target_step_id, self._get_default_settings_for_step_type(current_test_type))
            
            if current_test_type == 'flash':
                 port = config['flash_port']; baud = self.flash_baudrate.get()
                 files = {'bootloader': self.bootloader_path.get(), 'partitions': self.partitions_path.get(), 'app': self.app_path.get()}
                 esptool_args = build_esptool_args(port, baud, files)
                 if not esptool_args: raise ValueError("Flash-argumenttien muodostus epäonnistui.")
                 
                 is_shared_flash_port = port in self.shared_flash_queues and len(self.shared_flash_queues[port]) > 0
                 can_start_flash_now = not is_shared_flash_port or (self.shared_flash_queues[port][0] == device_idx)

                 if not can_start_flash_now:
                     runtime['last_status_msg'] = f"Odottaa Flash porttia ({port})..."
                     self._log_message(device_idx, f"Jonossa portille {port} ({current_test_display_name})...")
                     # Ei käynnistetä threadia, _trigger_queued_flash hoitaa tämän myöhemmin
                     return # Poistutaan tästä metodista, flash jonotetaan
                 
                 target_func = run_flash_worker; args = (device_idx, esptool_args, stop_event, self.gui_queue)
                 state['busy_flags']['flash_port'] = True
            
            elif current_test_type == 'serial':
                port = config['monitor_port']; baud = int(self.serial_baudrate.get())
                if not port: raise ValueError("Sarjaporttia ei ole määritetty.")
                target_func = run_serial_test_worker; args = (device_idx, port, baud, copy.deepcopy(step_settings), stop_event, self.gui_queue)
                state['busy_flags']['monitor_port'] = True
            
            elif current_test_type == 'daq':
                if not NIDAQMX_AVAILABLE: raise RuntimeError("DAQ-kirjasto (nidaqmx) puuttuu.")
                
                # --- DAQ-LUKON HANKINTA ---
                if not self.daq_lock.acquire(blocking=False):
                    holder = self.daq_in_use_by_device if self.daq_in_use_by_device is not None else "Tuntematon"
                    self._log_message(device_idx, f"DAQ varattu (Laite {holder}), lisätään jonoon...")
                    runtime['last_status_msg'] = f"Odottaa DAQ (varattu: D{holder})"
                    # Käytetään eventtiä, jonka _check_daq_queue voi triggeröidä
                    daq_request_event = threading.Event()
                    if not any(item[0] == device_idx for item in self.daq_wait_queue):
                        self.daq_wait_queue.append((device_idx, daq_request_event))
                    # Tärkeää: Älä jatka workerin käynnistykseen, koska lukkoa ei saatu.
                    # _check_daq_queue kutsuu _start_specific_test_step uudelleen, kun lukko vapautuu.
                    return
                
                # Jos päästiin tänne, lukko on saatu
                self.daq_in_use_by_device = device_idx
                state['busy_flags']['daq'] = True
                self._log_message(device_idx, "DAQ-lukko saatu.")
                
                target_func = run_daq_test_worker
                args = (device_idx, DAQ_DEVICE_NAME, copy.deepcopy(step_settings), stop_event, self.gui_queue)
            
            elif current_test_type == 'modbus':
                if not PYMODBUS_AVAILABLE: raise RuntimeError("Modbus-kirjasto (pymodbus) puuttuu.")
                port = config['modbus_port']
                if not port: raise ValueError("Modbus-porttia ei ole määritetty.")
                slave_id_val = int(config['modbus_slave_id']); baud_val = int(self.modbus_baudrate.get()); timeout_val = float(self.modbus_timeout.get())
                if slave_id_val <= 0: raise ValueError("Modbus Slave ID:n on oltava positiivinen.")
                target_func = run_modbus_test_worker; args = (device_idx, port, slave_id_val, copy.deepcopy(step_settings), baud_val, timeout_val, stop_event, self.gui_queue)
                state['busy_flags']['modbus_port'] = True
            
            elif current_test_type == 'wait_info':
                 target_func = run_wait_info_worker
                 args = (device_idx, copy.deepcopy(step_settings), stop_event, self.gui_queue)
            elif current_test_type == 'daq_and_serial' or current_test_type == 'daq_and_modbus':
                if not NIDAQMX_AVAILABLE:
                    raise RuntimeError("DAQ-kirjasto puuttuu yhdistelmätestiltä.")
                if current_test_type == 'daq_and_modbus' and not PYMODBUS_AVAILABLE:
                    raise RuntimeError("Modbus-kirjasto puuttuu yhdistelmätestiltä.")

                daq_s = step_settings.get('daq_settings')
                serial_s = step_settings.get('serial_settings') if current_test_type == 'daq_and_serial' else None
                modbus_s = step_settings.get('modbus_settings') if current_test_type == 'daq_and_modbus' else None

                if not daq_s or (current_test_type == 'daq_and_serial' and not serial_s) or \
                (current_test_type == 'daq_and_modbus' and not modbus_s):
                    raise ValueError(f"Asetukset puuttuvat yhdistelmätestille {current_test_type}")

                app_cfg_for_composite = {
                    'monitor_port': config['monitor_port'],
                    'serial_baudrate': self.serial_baudrate.get(),
                    'modbus_port': config['modbus_port'],
                    'modbus_slave_id': config['modbus_slave_id'],
                    'modbus_baudrate': self.modbus_baudrate.get(),
                    'modbus_timeout': self.modbus_timeout.get(),
                }
                
                # Välitä DAQ-lukko ja sen hallintafunktiot
                args = (device_idx, current_test_type,
                        copy.deepcopy(daq_s),
                        copy.deepcopy(serial_s) if serial_s else None,
                        copy.deepcopy(modbus_s) if modbus_s else None,
                        app_cfg_for_composite,
                        stop_event, self.gui_queue,
                        self.daq_lock, # Viite lukkoon
                        lambda: self.daq_in_use_by_device, # Funktio get
                        lambda dev_id: setattr(self, 'daq_in_use_by_device', dev_id), # Funktio set
                        lambda dev_id, evt: self.daq_wait_queue.append((dev_id, evt)), # Funktio add to queue
                        self._check_daq_queue # Funktio check queue
                        )
                target_func = run_composite_test_worker
            else:
                raise ValueError(f"Tuntematon testityyppi: {current_test_type}")

            if target_func:
                 thread = threading.Thread(target=target_func, args=args, kwargs=kwargs, daemon=True)
                 self.active_threads[(device_idx, target_step_id)] = thread
                 thread.start()
            else: # Ei pitäisi tapahtua, jos kaikki tyypit on katettu
                 raise Exception(f"Kohdefunktiota ei määritelty testityypille {current_test_type}")

        except Exception as e:
             error_msg = f"Virhe käynnistettäessä vaihetta {current_test_display_name} (ID: {target_step_id}): {e}"
             self._log_message(device_idx, error_msg, error=True)
             traceback.print_exc() # Tulosta koko traceback konsoliin debuggausta varten
             runtime['last_status_msg'] = "Käynnistysvirhe"
             runtime['steps_status'][target_step_id] = False # Merkitään epäonnistuneeksi
             self.test_results_ui.update_test_result(device_idx, target_step_id, False, running=False)
             
             # Vapauta busy flagit, jos ne ehdittiin asettaa TÄSSÄ vaiheessa
             if current_test_type == 'flash': state['busy_flags']['flash_port'] = False
             elif current_test_type == 'serial': state['busy_flags']['monitor_port'] = False
             elif current_test_type == 'modbus': state['busy_flags']['modbus_port'] = False
             elif current_test_type == 'daq': # DAQ-lukon vapautus, jos se ehdittiin varata
                 if self.daq_in_use_by_device == device_idx:
                    self.daq_in_use_by_device = None
                    state['busy_flags']['daq'] = False
                    try: self.daq_lock.release(); self._log_message(device_idx, "DAQ-lukko vapautettu käynnistysvirheen yhteydessä.")
                    except threading.ThreadError: pass # Oli jo vapaa
                    self.root.after(10, self._check_daq_queue) # Tarkista jono

             # Yritä siirtyä seuraavaan vaiheeseen, koska tämä epäonnistui jo käynnistyksessä
             self.root.after(50, lambda idx=device_idx: self._start_next_test_step(idx))

    def stop_all_tests(self): 
        stopped_count = 0
        for event in self.stop_events.values():
            if not event.is_set():
                event.set()
                stopped_count +=1
        messagebox.showinfo("Pysäytys", f"Pysäytyspyyntö lähetetty {stopped_count} aktiiviselle laitteelle." if stopped_count else "Ei aktiivisia testejä pysäytettäväksi.", parent=self.root)

    def _process_gui_queue(self): 
        try:
            while True: self._handle_gui_message(self.gui_queue.get_nowait())
        except queue.Empty: pass
        finally: self.root.after(100, self._process_gui_queue)

    def _handle_gui_message(self, message: Dict):
        msg_type = message.get('type')
        device_idx = message.get('device_index') # Voi olla None request_daq_lock-viestissä, jos se tulee eri tavalla
        data = message.get('data')

        # Varmista device_idx ja state, ellei kyseessä ole request_daq_lock, joka voi tulla ilman device_idx:ää datassa
        state = None
        runtime = None
        if device_idx is not None and device_idx in self.device_state:
            state = self.device_state[device_idx]
            runtime = state['runtime']
        elif msg_type != 'request_daq_lock': # Muut viestit vaativat validin device_idx:n
            print(f"Varoitus: Viesti ilman validia device_idx tai statea: {message}")
            return

        active_step_id_for_done_msg = runtime.get('current_stage_test_step_id') if runtime else None
        
        try:
            if msg_type == 'status':
                if runtime and runtime.get('sequence_running', False) and runtime.get('final_result') is None:
                    runtime['last_status_msg'] = str(data)
            elif msg_type in ['output', 'daq_log', 'modbus_log', 'serial_log']:
                self._log_message(device_idx if device_idx is not None else 0, str(data)) # device_idx 0 yleisille
            elif msg_type == 'clear_output':
                 if device_idx in self.log_text_widgets:
                     lw = self.log_text_widgets[device_idx]; lw.configure(state='normal'); lw.delete("1.0", "end"); lw.configure(state='disabled')
            
            elif msg_type.endswith('_done'):
                if not state or not runtime:
                    print(f"KRIITTINEN: Ei laitteen tilaa _done-viestille D{device_idx}. Viesti: {message}"); return

                done_test_type = msg_type.replace('_done', '')
                if done_test_type == 'serial_test': done_test_type = 'serial'
                
                result = bool(data) # Workerin palauttama tulos
                
                if not active_step_id_for_done_msg:
                    self._log_message(device_idx, f"KRIITTINEN: Vastaanotettu {msg_type}, mutta ei aktiivista vaiheen ID:tä. Data: {data}. Ohitetaan.", error=True)
                    if runtime['sequence_running']: self.root.after(100, lambda idx=device_idx: self._start_next_test_step(idx))
                    return

                step_config_for_done_step = next((s for s in self.test_order if s['id'] == active_step_id_for_done_msg), None)
                if not step_config_for_done_step:
                    self._log_message(device_idx, f"KRIITTINEN: Ei konfiguraatiota päättyneelle vaiheelle ID {active_step_id_for_done_msg}. Merkitään epäonnistuneeksi.", error=True)
                    runtime['steps_status'][active_step_id_for_done_msg] = False
                    self.test_results_ui.update_test_result(device_idx, active_step_id_for_done_msg, False, running=False)
                    if runtime['current_stage_test_step_id'] == active_step_id_for_done_msg: runtime['current_stage_test_step_id'] = None
                    self.root.after(10, lambda idx=device_idx: self._start_next_test_step(idx))
                    return

                display_name_for_log = step_config_for_done_step.get('name', f"Tuntematon vaihe ({done_test_type})")
                
                # Vapauta resurssit/liput ensin
                if done_test_type == 'flash': state['busy_flags']['flash_port'] = False
                elif done_test_type == 'serial': state['busy_flags']['monitor_port'] = False
                elif done_test_type == 'modbus': state['busy_flags']['modbus_port'] = False
                # DAQ-lukko vapautetaan erikseen release_daq_lock-viestillä workerista

                thread_key = (device_idx, active_step_id_for_done_msg)
                if thread_key in self.active_threads: del self.active_threads[thread_key]

                # Tarkista stop_event ennen uudelleenyrityslogiikkaa
                if device_idx in self.stop_events and self.stop_events[device_idx].is_set():
                    self._log_message(device_idx, f"Vaihe {display_name_for_log} {'onnistui' if result else 'epäonnistui'}, mutta testi keskeytetty. Ei jatkotoimenpiteitä tälle vaiheelle.")
                    runtime['steps_status'][active_step_id_for_done_msg] = False # Merkitään epäonnistuneeksi, jos keskeytetty
                    self.test_results_ui.update_test_result(device_idx, active_step_id_for_done_msg, False, running=False)
                    # _start_next_test_step kutsutaan lopussa, se hoitaa pysäytyksen loppuun
                    if runtime['current_stage_test_step_id'] == active_step_id_for_done_msg: runtime['current_stage_test_step_id'] = None
                    self.root.after(10, lambda idx=device_idx: self._start_next_test_step(idx))
                    return

                # Jos ei keskeytetty, jatka normaalilla tuloksen käsittelyllä ja uudelleenyrityksellä
                if result is True:
                    current_attempts = runtime['step_attempts'].get(active_step_id_for_done_msg, 0)
                    runtime['steps_status'][active_step_id_for_done_msg] = True
                    self.test_results_ui.update_test_result(device_idx, active_step_id_for_done_msg, True, running=False)
                    self._log_message(device_idx, f"--- Vaihe {display_name_for_log} valmis: OK (Yritys {current_attempts + 1}) ---")
                    if runtime['current_stage_test_step_id'] == active_step_id_for_done_msg: runtime['current_stage_test_step_id'] = None
                    self.root.after(10, lambda idx=device_idx: self._start_next_test_step(idx))
                else: # result is False
                    retry_enabled = step_config_for_done_step.get('retry_enabled', False)
                    max_retries = step_config_for_done_step.get('max_retries', 0)
                    retry_delay = step_config_for_done_step.get('retry_delay_s', self.default_retry_delay_s)
                    
                    runtime['step_attempts'][active_step_id_for_done_msg] = runtime['step_attempts'].get(active_step_id_for_done_msg, 0) + 1
                    new_attempt_count = runtime['step_attempts'][active_step_id_for_done_msg]

                    self._log_message(device_idx, f"--- Vaihe {display_name_for_log} epäonnistui (Yritys {new_attempt_count}/{max_retries + 1}) ---", error=True)
                    # Päivitä UI näyttämään epäonnistuminen, mutta pidä "running" jos yritetään uudelleen
                    self.test_results_ui.update_test_result(device_idx, active_step_id_for_done_msg, False, 
                                                           running=(retry_enabled and new_attempt_count <= max_retries))

                    if retry_enabled and new_attempt_count <= max_retries:
                        self._log_message(device_idx, f"Odotetaan {retry_delay:.1f}s ennen uudelleenyritystä vaiheelle '{display_name_for_log}'...")
                        runtime['steps_status'][active_step_id_for_done_msg] = None # Nollaa status uudelleenyritystä varten
                        # current_stage_test_step_id PYSYY samana!
                        self.root.after(int(retry_delay * 1000), 
                                        lambda idx=device_idx, step_id=active_step_id_for_done_msg: 
                                        self._start_specific_test_step(idx, step_id, is_retry=True))
                    else: # Ei uudelleenyritystä tai yritykset täynnä
                        if retry_enabled and new_attempt_count > max_retries:
                            self._log_message(device_idx, f"Maksimi uudelleenyritykset ({max_retries}) vaiheelle {display_name_for_log} saavutettu.")
                        runtime['steps_status'][active_step_id_for_done_msg] = False # Lopullinen epäonnistuminen
                        self.test_results_ui.update_test_result(device_idx, active_step_id_for_done_msg, False, running=False)
                        if runtime['current_stage_test_step_id'] == active_step_id_for_done_msg: runtime['current_stage_test_step_id'] = None
                        self.root.after(10, lambda idx=device_idx: self._start_next_test_step(idx))
            
            elif msg_type == 'request_daq_lock':
                # Tämä tulee nyt _start_specific_test_step (tai _start_next_test_step) -metodista
                # sen sijaan että se tulisi workerista.
                # Tässä ei pitäisi enää olla eventtiä datassa.
                requesting_device_idx = data.get('device_index') # Varmista, että tämä tulee datan mukana
                if requesting_device_idx is None:
                     self._log_message(0, "Virhe: DAQ-lukon pyyntö ilman device_indexiä datassa.", error=True); return

                # Päivitä state ja runtime tälle laitteelle
                if requesting_device_idx in self.device_state:
                    state = self.device_state[requesting_device_idx]
                    runtime = state['runtime']
                else:
                    self._log_message(requesting_device_idx, "Virhe: Ei laitteen tilaa DAQ-lukon pyynnölle.", error=True); return

                # Tässä kohtaa _start_specific_test_step odottaa tämän metodin loppuunsuorittamista
                # ja sen DAQ-lukon tilan asettamista.
                # Tämä logiikka on nyt siirretty _start_specific_test_step-metodiin.
                # Tämän viestin ei pitäisi enää tulla GUI-jonoon tällä tavalla.
                # Pidetään tämä haara siltä varalta, että jokin vanha logiikka vielä lähettää sen.
                self._log_message(requesting_device_idx, "Vastaanotettu vanhentunut 'request_daq_lock' GUI-jonossa. Ohitetaan.", error=True)


            elif msg_type == 'release_daq_lock': # Tämä tulee workerin finally-lohkosta
                releasing_device_idx = message.get('device_index')
                if releasing_device_idx is None:
                    self._log_message(0, "Virhe: DAQ-lukon vapautuspyyntö ilman device_idx:ää.", error=True); return
                
                if releasing_device_idx in self.device_state:
                    self.device_state[releasing_device_idx]['busy_flags']['daq'] = False
                
                if self.daq_in_use_by_device == releasing_device_idx:
                    self.daq_in_use_by_device = None
                    try:
                        self.daq_lock.release()
                        self._log_message(releasing_device_idx, "DAQ-lukko vapautettu (release_daq_lock-viesti).")
                    except threading.ThreadError:
                        self._log_message(releasing_device_idx, "Varoitus: DAQ-lukon vapautus epäonnistui (ei omistettu tai jo vapaa).")
                    except Exception as e_rl:
                        self._log_message(releasing_device_idx, f"Virhe DAQ-lukon vapautuksessa: {e_rl}", error=True)
                    self.root.after(10, self._check_daq_queue) # Tarkista jono heti
                elif self.daq_in_use_by_device is not None: # Vapautusyritys, mutta joku muu omistaa
                    self._log_message(releasing_device_idx, f"Varoitus: Laite {releasing_device_idx} yritti vapauttaa DAQ-lukkoa, omistaja Laite {self.daq_in_use_by_device}.", error=True)
                else: # Lukko oli jo vapaa
                     self._log_message(releasing_device_idx, "Huom: DAQ-lukko oli jo vapaa vapautuspyynnön saapuessa.")
                     self.root.after(10, self._check_daq_queue) # Tarkista jono silti
            elif msg_type == 'busy_flag_update':
                if state and 'busy_flags' in state:
                    flag_name = message.get('flag_name')
                    value = message.get('value')
                    if flag_name in state['busy_flags']:
                        state['busy_flags'][flag_name] = value
                        # self._log_message(device_idx, f"Busy flag '{flag_name}' asetettu: {value}") # Voi olla liian verbose
                    else:
                        self._log_message(device_idx, f"Tuntematon busy flag päivitys: {flag_name}", error=True)
            else:
                print(f"Varoitus: Tuntematon GUI-viesti: {msg_type} laitteelle {device_idx}")
        except KeyError as e:
            print(f"KeyError GUI-viestissä ({msg_type} D{device_idx}): {e}, Viesti: {message}")
            traceback.print_exc()
        except Exception as e:
            print(f"Virhe GUI-viestin ({msg_type} D{device_idx}) käsittelyssä: {e}, Viesti: {message}")
            traceback.print_exc()

    def _check_daq_queue(self):
        if not self.daq_in_use_by_device and self.daq_wait_queue:
            # Ota seuraava jonosta, mutta älä poista heti, jos lukon saanti epäonnistuu
            device_idx_to_try, associated_event_ignored = self.daq_wait_queue[0] 

            if device_idx_to_try in self.device_state and \
               self.device_state[device_idx_to_try]['runtime'].get('sequence_running', False) and \
               not (self.stop_events.get(device_idx_to_try) and self.stop_events[device_idx_to_try].is_set()):
                
                self._log_message(device_idx_to_try, f"Yritetään antaa DAQ-lukko jonossa olevalle laitteelle {device_idx_to_try}...")
                
                # Yritä _start_specific_test_step uudelleen tälle laitteelle ja vaiheelle.
                # Sen DAQ-haara yrittää hankkia lukon.
                # Tarvitsemme tiedon, mikä vaihe oli kesken tällä laitteella.
                current_step_for_queued_device = self.device_state[device_idx_to_try]['runtime'].get('current_stage_test_step_id')
                if current_step_for_queued_device:
                    # Poista jonosta vasta, kun tiedetään, että lukko voidaan yrittää antaa
                    self.daq_wait_queue.pop(0)
                    self.root.after(10, lambda idx=device_idx_to_try, step_id=current_step_for_queued_device: 
                                    self._start_specific_test_step(idx, step_id, is_retry=False)) # is_retry=False, koska tämä on jonosta jatkaminen
                else:
                    self._log_message(device_idx_to_try, f"Ei aktiivista vaihetta jonossa olevalle laitteelle {device_idx_to_try}, poistetaan jonosta.", error=True)
                    if self.daq_wait_queue and self.daq_wait_queue[0][0] == device_idx_to_try: # Varmista, että se on edelleen jonon kärjessä
                        self.daq_wait_queue.pop(0)
                    if self.daq_wait_queue: # Jos jonossa on vielä muita, yritä seuraavaa
                         self.root.after(10, self._check_daq_queue)

            else: # Jonossa oleva laite ei ole enää validi (esim. pysäytetty)
                self._log_message(device_idx_to_try, f"Jonossa oleva laite {device_idx_to_try} ei ole enää aktiivinen. Poistetaan jonosta.")
                if self.daq_wait_queue and self.daq_wait_queue[0][0] == device_idx_to_try:
                    self.daq_wait_queue.pop(0)
                if self.daq_wait_queue: # Yritä seuraavaa
                    self.root.after(10, self._check_daq_queue)
 
    def _trigger_queued_flash(self, device_idx: int):
        if device_idx not in self.device_state or device_idx not in self.stop_events: 
            print(f"DEBUG: Cannot trigger queued flash for missing/stopped device {device_idx}")
            return

        state = self.device_state[device_idx]; config = state['config']; runtime = state['runtime']
        stop_event = self.stop_events[device_idx]

        flash_port_of_this_device = config.get('flash_port') 

        if flash_port_of_this_device and flash_port_of_this_device in self.shared_flash_queues:
            queue_for_port = self.shared_flash_queues[flash_port_of_this_device]
            if not queue_for_port or queue_for_port[0] != device_idx:
                print(f"DEBUG: D{device_idx} is no longer at the head of queue for port {flash_port_of_this_device}. Current head: {queue_for_port[0] if queue_for_port else 'None'}. Aborting trigger.")
                if queue_for_port: 
                    self.root.after(10, lambda nidx=queue_for_port[0]: self._trigger_queued_flash(nidx))
                return 
        elif flash_port_of_this_device: 
            print(f"DEBUG: D{device_idx} triggered for flash on port {flash_port_of_this_device}, but port not in shared queues.")
        
        flash_step_to_run_config = next((s_cfg for s_cfg in self.test_order if s_cfg['type'] == 'flash' and config['tests_enabled'].get(s_cfg['id']) and runtime['steps_status'].get(s_cfg['id']) is None), None)
        
        if not flash_step_to_run_config:
            self._log_message(device_idx, f"Ei ajettavia flash-vaiheita jonosta laitteelle {config['name']}.")
            if flash_port_of_this_device and flash_port_of_this_device in self.shared_flash_queues:
                qfp = self.shared_flash_queues[flash_port_of_this_device]
                if device_idx in qfp: qfp.remove(device_idx)
                if not qfp: del self.shared_flash_queues[flash_port_of_this_device]
                elif qfp: self.root.after(10, lambda nidx=qfp[0]: self._trigger_queued_flash(nidx))
            if runtime['sequence_running']: self.root.after(10, lambda idx=device_idx: self._start_next_test_step(idx))
            return

        current_flash_step_id = flash_step_to_run_config['id']
        current_flash_display_name = flash_step_to_run_config.get('name', 'Flash')

        if stop_event.is_set():
            runtime['last_status_msg'] = "Pysäytetty (jonossa)"; runtime['steps_status'][current_flash_step_id] = None
            self._log_message(device_idx, f"{current_flash_display_name} peruutettu jonosta pysäytyspyynnön vuoksi.")
            if flash_port_of_this_device and flash_port_of_this_device in self.shared_flash_queues:
                qfp = self.shared_flash_queues[flash_port_of_this_device]
                if device_idx in qfp: qfp.remove(device_idx)
                if not qfp: del self.shared_flash_queues[flash_port_of_this_device]
                elif qfp: self.root.after(10, lambda nidx=qfp[0]: self._trigger_queued_flash(nidx))
            if runtime['sequence_running']: self.root.after(10, lambda idx=device_idx: self._start_next_test_step(idx))
            return

        runtime['current_stage_test_step_id'] = current_flash_step_id 
        runtime['last_status_msg'] = f"Käynnistetään {current_flash_display_name} ({config['flash_port']})..."
        self.test_results_ui.update_test_result(device_idx, current_flash_step_id, None, running=True)
        self._log_message(device_idx, f"--- Aloitetaan vaihe: {current_flash_display_name} (jonosta portille {config['flash_port']}) ---")
        try:
            port = config['flash_port']; baud = self.flash_baudrate.get()
            files = {'bootloader': self.bootloader_path.get(), 'partitions': self.partitions_path.get(), 'app': self.app_path.get()}
            esptool_args = build_esptool_args(port, baud, files)
            if not esptool_args: raise ValueError("Flash-argumentit epäonnistuivat (jonosta).")
            args = (device_idx, esptool_args, stop_event, self.gui_queue); state['busy_flags']['flash_port'] = True
            thread = threading.Thread(target=run_flash_worker, args=args, daemon=True)
            self.active_threads[(device_idx, current_flash_step_id)] = thread; thread.start()
        except Exception as e:
            error_msg = f"Virhe käynnistettäessä jonossa ollutta {current_flash_display_name}: {e}"
            self._log_message(device_idx, error_msg, error=True)
            runtime['steps_status'][current_flash_step_id] = False
            self.test_results_ui.update_test_result(device_idx, current_flash_step_id, False, running=False)
            runtime['last_status_msg'] = f"{current_flash_display_name} Virhe (jono)"; state['busy_flags']['flash_port'] = False
            
            if flash_port_of_this_device and flash_port_of_this_device in self.shared_flash_queues:
                qfp = self.shared_flash_queues[flash_port_of_this_device]
                if device_idx in qfp: qfp.remove(device_idx)
                if not qfp: del self.shared_flash_queues[flash_port_of_this_device]
                elif qfp: self.root.after(10, lambda nidx=qfp[0]: self._trigger_queued_flash(nidx))
            
            self.root.after(50, lambda idx=device_idx: self._start_next_test_step(idx))

    def _log_messageVANHA(self, device_idx: int, message: str, error: bool = False):
        ts = time.strftime("%H:%M:%S")
        pfx = "ERROR: " if error else ""
        log_line = f"{ts} {pfx}{message}"

        # Lisää loki-widgettiin GUI:ssa
        if device_idx in self.log_text_widgets:
            lw = self.log_text_widgets[device_idx]
            try:
                current_state = lw.cget("state")
                lw.configure(state='normal')
                lw.insert("end", log_line + "\n")
                lw.see("end")
                lw.configure(state=current_state)
            except Exception as e:
                print(f"Error logging to GUI D{device_idx}: {e}")
        else:
            print(f"Log Widget Missing D{device_idx}: {log_line}")

        # Lisää lokipuskuriin automaattista tallennusta varten
        if device_idx not in self.device_log_buffer:
            self.device_log_buffer[device_idx] = []
        self.device_log_buffer[device_idx].append((ts, f"{pfx}{message}"))

    #UUSI
    def _log_message(self, device_idx: int, message: str, error: bool = False):
        ts = time.strftime("%H:%M:%S")
        pfx = "ERROR: " if error else ""
        log_line = f"{ts} {pfx}{message}"

        # Lisää loki-widgettiin GUI:ssa
        if device_idx in self.log_text_widgets:
            lw = self.log_text_widgets[device_idx]
            try:
                lw.configure(state='normal')  # Aseta tila muokattavaksi
                lw.insert("end", log_line + "\n")
                lw.see("end")
                lw.configure(state='disabled') # Aseta tila vain luku -muotoon
            except Exception as e:
                # Tulosta tarkempi virheilmoitus, jos jotain menee vielä pieleen
                print(f"Yksityiskohtainen virhe lokituksessa GUI:hin D{device_idx} (widget: {type(lw)}): {e}")
                import traceback
                traceback.print_exc() # Näyttää koko virheen jäljityksen
        elif device_idx != 0: # Älä tulosta "Log Widget Missing" yleisille viesteille (D0),
                              # ellei niille ole tarkoituksella tehty omaa paikkaa.
            print(f"Log Widget Missing D{device_idx}: {log_line}")
        else: # Jos device_idx on 0 (yleinen viesti) ja sille ei ole widgettiä, tulosta se vain konsoliin.
            print(f"Yleinen Loki: {log_line}")


        # Lisää lokipuskuriin automaattista tallennusta varten
        if device_idx not in self.device_log_buffer:
            self.device_log_buffer[device_idx] = []
        self.device_log_buffer[device_idx].append((ts, f"{pfx}{message}"))

    def _update_ui_for_final_result(self, device_idx): 
        state = self.device_state[device_idx]; runtime = state['runtime']
        print(f"Laite {device_idx} ({self.device_state[device_idx]['config']['name']}) valmis. Tulos: {self.device_state[device_idx]['runtime']['final_result']}")

        # Tarkista, ovatko KAIKKI laitteet valmiita, ja tallenna lokit jos ovat
        if self._all_tests_completed_or_stopped():
            self.root.after(500, self._write_all_logs_to_csv) # Pieni viive varmuuden vuoksi

    def save_all_settings(self):
        self._update_device_names_in_ui()
        self._update_config_from_ui_for_save_or_start() 

        fp = filedialog.asksaveasfilename(
            defaultextension=".json", filetypes=[("Test Config JSON","*.json")], 
            title="Tallenna Kokoonpano", parent=self.root
        )
        if not fp: return
        try:
            settings_to_save = {
                "version": 3, 
                "global_settings": {
                    "bootloader_path": self.bootloader_path.get(),    
                    "partitions_path": self.partitions_path.get(),  
                    "app_path": self.app_path.get(),                
                    "flash_baudrate": self.flash_baudrate.get(),
                    "serial_baudrate": self.serial_baudrate.get(),
                    "modbus_baudrate": self.modbus_baudrate.get(),
                    "modbus_timeout": self.modbus_timeout.get(),
                    "flash_in_sequence": self.flash_in_sequence_var.get() 
                },
                "test_order": self.test_order, 
                "step_specific_settings": self.step_specific_settings,
                "device_configurations": {}, 
                "layout": {"device_count": self.current_device_count}
            }
            for i in range(1, self.current_device_count + 1):
                if i in self.device_state:
                    settings_to_save["device_configurations"][str(i)] = copy.deepcopy(self.device_state[i]['config'])
            with open(fp, 'w', encoding='utf-8') as f: json.dump(settings_to_save, f, indent=2, ensure_ascii=False)
            messagebox.showinfo("Tallennettu", f"Asetukset tallennettu:\n{fp}", parent=self.root)
        except Exception as e: 
            messagebox.showerror("Tallennusvirhe", f"Tallennus epäonnistui:\n{e}", parent=self.root)
            import traceback
            traceback.print_exc()

    def load_all_settings(self): 
        fp = filedialog.askopenfilename(
            defaultextension=".json", filetypes=[("Test Config JSON","*.json")],
            title="Lataa Kokoonpano", parent=self.root
        )
        if not fp: return
        try:
            with open(fp, 'r', encoding='utf-8') as f: loaded_settings = json.load(f)
            
            g_settings = loaded_settings.get("global_settings", {})
            self.bootloader_path.set(g_settings.get("bootloader_path", "")); self.partitions_path.set(g_settings.get("partitions_path", ""))
            self.app_path.set(g_settings.get("app_path", "")); self.flash_baudrate.set(g_settings.get("flash_baudrate", DEFAULT_BAUD_RATE_FLASH))
            self.serial_baudrate.set(g_settings.get("serial_baudrate", DEFAULT_BAUD_RATE_SERIAL))
            self.modbus_baudrate.set(g_settings.get("modbus_baudrate", str(DEFAULT_MODBUS_BAUDRATE)))
            self.modbus_timeout.set(g_settings.get("modbus_timeout", str(DEFAULT_MODBUS_TIMEOUT)))
            self.flash_in_sequence_var.set(g_settings.get("flash_in_sequence", True)) 

            self.test_order = copy.deepcopy(loaded_settings.get("test_order", self._get_default_test_order()))
            for step in self.test_order:
                if 'id' not in step: step['id'] = str(uuid.uuid4())
                if 'type' not in step: step['type'] = 'unknown' 

            self.step_specific_settings = copy.deepcopy(loaded_settings.get("step_specific_settings", {}))
            self._ensure_step_specific_settings() 

            new_count = max(1, min(loaded_settings.get("layout", {}).get("device_count", self.current_device_count), MAX_DEVICES))
            self.current_device_count = new_count 
            self.device_state.clear() 
            loaded_dev_configs = loaded_settings.get("device_configurations", {})
            for i in range(1, self.current_device_count + 1):
                self.device_state[i] = self._create_default_device_state(i) 
                dev_idx_str = str(i)
                if dev_idx_str in loaded_dev_configs:
                    loaded_config_for_dev = loaded_dev_configs[dev_idx_str]
                    for key in self.device_state[i]['config'].keys():
                        if key == 'tests_enabled':
                            for step_id_from_file, enabled_value in loaded_config_for_dev.get('tests_enabled', {}).items():
                                if step_id_from_file in self.device_state[i]['config']['tests_enabled']:
                                    self.device_state[i]['config']['tests_enabled'][step_id_from_file] = enabled_value
                        elif key in loaded_config_for_dev:
                                self.device_state[i]['config'][key] = loaded_config_for_dev[key]
            
            self.refresh_ports()
            self._create_device_frames_widgets() 
            self._update_results_ui_layout()
            self._update_log_tabs()
            messagebox.showinfo("Ladattu", f"Asetukset ladattu:\n{fp}", parent=self.root)
        
        except json.JSONDecodeError: messagebox.showerror("Latausvirhe", "JSON-tiedosto virheellinen.", parent=self.root)
        except Exception as e: messagebox.showerror("Latausvirhe", f"Lataus epäonnistui:\n{e}", parent=self.root); import traceback; traceback.print_exc()
 
    def on_closing(self):
        if any(state['runtime']['sequence_running'] for state in self.device_state.values()):
            if messagebox.askyesno("Lopeta?", "Testit käynnissä. Lopeta silti?", parent=self.root):
                self.stop_all_tests() # Tämä kutsuu _write_all_logs_to_csv:n (viiveellä)
                self.root.destroy()
            else:
                return # Älä sulje
        else:
            # Jos testit eivät ole käynnissä, mutta lokitiedostoa on ehkä aloitettu, tallennetaan
            if self.current_test_run_log_filename and self.device_log_buffer:
                self._log_message(0, "Sovellus suljetaan. Tallennetaan keskeneräisen testiajon lokit.")
                self._write_all_logs_to_csv() # Tallennetaan heti ilman viivettä
        self.root.destroy()

    def _write_all_logs_to_csv(self):
        if not self.current_test_run_log_filename or not self.device_log_buffer:
            print("Ei lokitietoja tallennettavaksi tai tiedostonimeä ei asetettu.")
            return

        filepath = self.current_test_run_log_filename
        print(f"Tallennetaan kaikkien laitteiden lokit tiedostoon: {filepath}")
        try:
            with open(filepath, 'w', newline='', encoding='utf-8-sig') as csvfile:
                csv_writer = csv.writer(csvfile, delimiter=';')
                csv_writer.writerow(["Aikaleima", "Laitteen Indeksi", "Laitteen Nimi", "Viesti"])

                # Käydään läpi laitteet järjestyksessä, jos mahdollista
                sorted_device_indices = sorted(self.device_log_buffer.keys())

                for device_idx in sorted_device_indices:
                    device_name = self.device_state.get(device_idx, {}).get('config', {}).get('name', f"Laite {device_idx}")
                    if device_idx == 0: # Yleiset lokiviestit
                        device_name = "Yleinen"

                    for timestamp_str, message_str in self.device_log_buffer[device_idx]:
                        csv_writer.writerow([timestamp_str, str(device_idx) if device_idx != 0 else "", device_name, message_str])
            
            self._log_message(0, f"Kaikki lokit tallennettu onnistuneesti: {filepath}") # Yleinen lokiviesti
            # messagebox.showinfo("Lokit Tallennettu", f"Kaikki lokit tallennettu tiedostoon:\n{filepath}", parent=self.root)
            self.current_test_run_log_filename = None # Nollaa seuraavaa ajoa varten
            self.device_log_buffer.clear()

        except Exception as e:
            error_msg = f"Kaikkien lokien tallennus epäonnistui: {e}"
            print(error_msg)
            if hasattr(self, '_log_message'): # Varmista, että metodi on olemassa
                 self._log_message(0, error_msg, error=True) # Yleinen lokivirhe
            # messagebox.showerror("Tallennusvirhe", error_msg, parent=self.root)
 
    def _all_tests_completed_or_stopped(self) -> bool:
        """Tarkistaa, ovatko kaikkien laitteiden testisekvenssit päättyneet."""
        if not self.device_state: # Jos laitteita ei ole, katsotaan valmiiksi
            return True
        for i in range(1, self.current_device_count + 1):
            if i in self.device_state and self.device_state[i]['runtime'].get('sequence_running', False):
                return False # Vähintään yksi laite vielä testaa
        return True # Kaikki laitteet ovat lopettaneet
   
    #EI TOIMI
    def _update_log_tabsUUSI(self): # Poistetaan CSV-tallennuspainike välilehdiltä
        if not hasattr(self, 'log_tabview') or self.log_tabview._segmented_button is None: return
        
        active_tab_name = None
        try:
            if self.log_tabview.get(): active_tab_name = self.log_tabview.get()
        except: pass

        current_tab_names = list(self.log_tabview._tab_dict.keys())
        for tab_name in current_tab_names:
            try: self.log_tabview.delete(tab_name)
            except Exception as e: print(f"Varoitus: Ei voitu poistaa logitabiä '{tab_name}': {e}")
        self.log_text_widgets.clear()

        new_tab_names = []
        for i in range(self.current_device_count):
            device_idx = i + 1
            dev_name = self.device_state.get(device_idx, {}).get('config',{}).get('name', f"Laite {device_idx}")
            unique_name = dev_name
            # ... (uniikin nimen generointi tarvittaessa) ...
            new_tab_names.append(unique_name)

            try:
                self.log_tabview.add(unique_name)
                tab_frame = self.log_tabview.tab(unique_name)
                if tab_frame:
                    tab_frame.grid_rowconfigure(0, weight=1) # Tekstikenttä täyttää koko tilan
                    tab_frame.grid_columnconfigure(0, weight=1)

                    text_widget = ctk.CTkTextbox(tab_frame, wrap="word", height=10, font=("Courier New", 9))
                    text_widget.grid(row=0, column=0, sticky="nsew", padx=5, pady=5) # Vain tekstikenttä
                    text_widget.configure(state='disabled')
                    self.log_text_widgets[device_idx] = text_widget
            except Exception as e:
                print(f"Virhe logitabin luonnissa '{unique_name}': {e}")

        if active_tab_name and active_tab_name in self.log_tabview._name_list:
            try: self.log_tabview.set(active_tab_name)
            except: pass
        elif new_tab_names:
            try: self.log_tabview.set(new_tab_names[0])
            except: pass

def main():
    root = ctk.CTk()
    app = TestiOhjelmaApp(root)
    root.protocol("WM_DELETE_WINDOW", app.on_closing)
    root.mainloop()

if __name__ == "__main__":
    main()



