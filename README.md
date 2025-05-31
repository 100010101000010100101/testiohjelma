"""
OIKEAT:

  main6_2.py





    Below are the **modified and added functions/methods** designed to address these problems and create a working structure. I've focused on:

    1.  **Unifying UI:** Using only one consistent way to build the device settings UI.
    2.  **Centralized State:** Using `self.device_state` to manage configuration and runtime status for each device.
    3.  **Implementing Sequencing:** Adding the crucial `_start_next_test_step` logic.
    4.  **Fixing UI Updates:** Correcting how `TestResultsUI` is updated.
    5.  **Correcting State Management:** Initializing variables, fixing stop logic, etc.

    **Important Notes:**

    *   This is still complex code. Thorough testing in your specific hardware environment is essential.
    *   Error handling within worker threads might need further refinement based on real-world device responses.
    *   The DAQ and Modbus configuration windows (`DAQConfigWindow`, `ModbusConfigWindow`) and the worker functions (`run_daq_test`, `run_modbus_test_worker`, etc.) themselves are assumed to be *mostly* correct from the previous analysis, except for how their results are stored and used by the main app.
    *   I've added default settings directly in `__init__` for simplicity, but loading them from a default file might be better in a production tool.


    **Modified/New Code Sections for `TestiOhjelmaApp` and `TestResultsUI`:**

    **Key Changes and Explanations:**

    1.  **`__init__`:**
        *   Removed variables associated with the dead UI path (`_var` suffixes, `device_widgets`, etc.).
        *   Initialized `self.device_state`, `self.active_threads`, `self.stop_events`.
        *   Initialized DAQ lock variables (`self.daq_lock`, `self.daq_in_use_by_device`, `self.daq_wait_queue`).
        *   Added storage for actual test settings (`self.daq_settings`, `self.modbus_sequence`, `self.serial_settings`) and initialized them with defaults using helper methods.
        *   Defined `TEST_ORDER` and `TEST_NAMES` constants.
        *   Calls `_initialize_device_states()` to populate the state for the initial `current_device_count`.
        *   Calls `_update_results_ui_layout()` to draw the initial grid.



    2.  **UI Creation (`_create_left_panel`, `_create_device_frames_widgets`):**
        *   Removed `create_settings_panel`, `create_device_settings`.
        *   `_create_device_frames_widgets` now creates the UI based *only* on `self.current_device_count` and reads initial values from `self.device_state`.
        *   **Crucially added Test Selection Checkboxes** to each device frame, storing their `BooleanVar`s in `self.test_selection_vars`. Disabled DAQ/Modbus checkboxes if libraries are missing.
        *   Layout adjusted using `grid` and `pack` for better spacing and resizing. Added scrollable canvas handling.



    3.  **Configuration Windows (`open_..._config`):**
        *   Now correctly retrieves settings/sequences using `get_settings`/`get_sequence` and stores them in the respective instance variables (`self.daq_settings`, etc.). Used `deepcopy` when getting/setting defaults.



    4.  **`start_tests`:**
        *   Completely rewritten.
        *   Validates global file paths *only* if the Flash test is selected anywhere.
        *   Iterates through devices, reads config directly from UI `StringVar`s/`BooleanVar`s, and stores it in `self.device_state[i]['config']`.
        *   Performs validation (required ports for *selected* tests, port conflicts, slave ID format).
        *   Displays validation errors clearly.
        *   Resets UI (`TestResultsUI`) and runtime state (`self.device_state[i]['runtime']`, `busy_flags`) for devices that will run.
        *   Clears/creates `stop_events` dictionary.
        *   Calls `_start_next_test_step` for each device to begin its sequence.

    5.  **`_start_next_test_step`:**
        *   **This is the new core sequencing engine.**
        *   Checks the device's stop event.
        *   Finds the next test in `TEST_ORDER` that is enabled in the device's config and has not yet run (status is `None`).
        *   If no more tests, calculates the final result (PASS/FAIL/INCOMPLETE/STOPPED) and updates state/UI.
        *   If a test is found:
            *   Updates status, logs start message.
            *   Prepares arguments for the correct worker function based on `test_id`, pulling config from `self.device_state[device_idx]['config']` and global settings (baud rates, file paths, test settings).
            *   Handles DAQ locking implicitly (worker requests lock via queue).
            *   Starts the worker thread, storing it in `self.active_threads`.
            *   Includes basic error handling for thread creation/argument preparation failures.

    6.  **`stop_all_tests`:**
        *   Correctly iterates through the `self.stop_events` dictionary and calls `set()` on each device's event.

    7.  **`_handle_gui_message`:**
        *   Refined to work with the new state structure.
        *   On `*_done` messages:
            *   Updates `steps_status` in `self.device_state`.
            *   Updates the `TestResultsUI` using the corrected method.
            *   Resets busy flags.
            *   Removes the thread from `active_threads`.
            *   **Crucially calls `_start_next_test_step`** to continue the sequence.
            *   Handles finalization if the sequence was stopped during the step.
        *   Handles DAQ lock requests/releases using the queue and the central lock attributes.
        *   Uses specific log message types (`daq_log`, `modbus_log`) to potentially filter later.

    8.  **`TestResultsUI`:**
        *   Rewritten `create_results_grid` to correctly build the grid and store references.
        *   Rewritten `update_test_result` to find the correct canvas using the `(device_idx, test_idx)` key and update its oval/text items. Added a `running` state.
        *   Added `update_device_names` to update headers.
        *   `reset_results` now iterates through `result_canvases`.

    9.  **Logging (`_log_message`):**
        *   Added basic timestamping.
        *   Ensures widget is enabled before inserting text and disabled afterwards.

    10. **Saving/Loading:**
        *   Rewritten to save/load the unified state (`global_settings`, `test_settings`, `device_configurations` containing the `config` part of `self.device_state`).
        *   Correctly restores device count, names, ports, and *test selections* from the loaded file.
        *   Rebuilds the UI based on the loaded state.

    11. **Closing (`on_closing`):**
        *   Added a handler to ask the user before closing if tests are running.

    This corrected structure provides a much more robust foundation for the testing application. Remember to replace the placeholder worker functions with your actual, corrected versions.
"""


Tärkeimmät muutokset TestOrderConfigWindowssa:

_populate_scrollable_list:
create_command_for_index(idx) sisäfunktio: 
    - Tämä on kriittinen korjaus. Kun luodaan komentoja silmukassa lambdoilla, lambda sitoo muuttujan arvon vasta, kun sitä kutsutaan. Ilman tätä apufunktiota kaikki napit kutsuisivat _select_itemiä viimeisimmällä i:n arvolla. Apufunktio "vangitsee" oikean idx:n arvon jokaiselle napille.
    - Valitun itemin uudelleenvalinta current_selection_value:n avulla, kun lista populoidaan uudelleen.

_select_item:
    - Poistettu button_widget-parametri, koska sitä ei tarvita, jos self.list_item_widgets on oikein populointu.
    - Korostusvärit yrittävät nyt käyttää CustomTkinterin teemamanagerista haettuja värejä (hover_color, text_color_disabled, text_color labelille), jotta ne sopivat paremmin teemaan. Voit joutua vielä hienosäätämään näitä.
_get_selected_index: 
    - Ei enää näytä messageboxia, jos mitään ei ole valittu, koska tämä voi olla normaalia. Toimintonapit (move, remove) voivat näyttää varoituksen tarvittaessa.
_move_up, _move_down, _remove_selected: 
    - Lisätty tarkistus, että jokin item on valittu, ja näytetään messagebox, jos ei ole.
    
Mitä sinun tulee tehdä:
Korvaa koko TestOrderConfigWindow-luokka koodissasi yllä olevalla versiolla.
Varmista, että TestiOhjelmaApp-luokan _update_config_from_ui_for_save_or_start, start_tests ja save_all_settings -metodit on päivitetty sisältämään ehdotetut COM-porttien tallennuslogiikat (kuten edellisessä vastauksessasi).
Varmista, että SerialConfigWindow-luokka on päivitetty sisältämään Tallenna/Lataa/OK/Peruuta-painikkeet ja niihin liittyvät metodit (kuten edellisessä vastauksessasi).
Tarkista worker-funktioiden kutsut: Varmista, että self.gui_queue välitetään oikein worker-funktioille _start_next_test_step ja _trigger_queued_flash -metodeissa.
Nyt TestOrderConfigWindow-ikkunan pitäisi toimia oikein:
Listan pitäisi näyttää testivaiheet.
Klikkaamalla vaihetta sen pitäisi korostua.
Siirrä ylös/alas ja poista -napit pitäisi toimia valitulle vaiheelle.
Uusien vaiheiden lisäämisen pitäisi toimia.






# Koko TestOrderConfigWindow-luokka korjattuna ja täydennettynä
class TestOrderConfigWindow(ctk.CTkToplevel):
    def __init__(self, parent, current_test_order: List[Dict[str, str]]):
        super().__init__(parent)
        self.title("Määritä Testijärjestys")
        self.parent = parent
        self.grab_set()
        self.transient(parent)
        self.test_order = copy.deepcopy(current_test_order)
        self.result_test_order = None
        self.selected_index_var = ctk.IntVar(value=-1) 

        self._create_widgets()
        self._populate_scrollable_list() 

        self.geometry("600x500") # Hieman leveämpi ja korkeampi
        self.geometry(f"+{parent.winfo_rootx()+100}+{parent.winfo_rooty()+100}")
        self.protocol("WM_DELETE_WINDOW", self._on_cancel)
        self.wait_window(self)

    def _create_widgets(self):
        main_frame = ctk.CTkFrame(self, corner_radius=0)
        main_frame.pack(fill="both", expand=True, padx=10, pady=10)

        list_container_frame = ctk.CTkFrame(main_frame)
        list_container_frame.pack(fill="both", expand=True, pady=5)
        ctk.CTkLabel(list_container_frame, text="Nykyinen järjestys (klikkaa vaihetta valitaksesi):").pack(anchor="w", padx=5)
        
        self.scrollable_list_frame = ctk.CTkScrollableFrame(list_container_frame, label_text=None)
        self.scrollable_list_frame.pack(fill="both", expand=True, pady=5)
        self.list_item_widgets: List[Tuple[ctk.CTkFrame, ctk.CTkButton]] = [] 

        mod_frame = ctk.CTkFrame(main_frame)
        mod_frame.pack(fill="x", pady=5)
        ctk.CTkButton(mod_frame, text="Siirrä Ylös ▲", command=self._move_up).pack(side="left", padx=5)
        ctk.CTkButton(mod_frame, text="Siirrä Alas ▼", command=self._move_down).pack(side="left", padx=5)
        ctk.CTkButton(mod_frame, text="Poista Valittu", command=self._remove_selected).pack(side="left", padx=5)

        add_frame = ctk.CTkFrame(main_frame)
        add_frame.pack(fill="x", pady=5)
        ctk.CTkLabel(add_frame, text="Lisää uusi testivaihe:").pack(anchor="w", padx=5)
        
        available_types = list(AVAILABLE_TEST_TYPES.keys())
        self.new_test_type_var = ctk.StringVar(value=available_types[0] if available_types else "")
        ctk.CTkComboBox(add_frame, variable=self.new_test_type_var, values=available_types).pack(side="left", padx=5)
        
        self.new_test_name_var = ctk.StringVar()
        ctk.CTkEntry(add_frame, textvariable=self.new_test_name_var, placeholder_text="Vaiheen nimi (vapaaehtoinen)").pack(side="left", padx=5, expand=True, fill="x")
        ctk.CTkButton(add_frame, text="Lisää Vaihe", command=self._add_step).pack(side="left", padx=5)

        bottom_frame = ctk.CTkFrame(main_frame)
        bottom_frame.pack(fill="x", pady=(10,0))
        ctk.CTkButton(bottom_frame, text="Peruuta", command=self._on_cancel).pack(side="right", padx=5)
        ctk.CTkButton(bottom_frame, text="OK", command=self._on_ok, fg_color="green").pack(side="right", padx=5)

    def _select_item(self, index: int):
        self.selected_index_var.set(index)
        for i, (frame, btn) in enumerate(self.list_item_widgets):
            # Käytetään teeman mukaisia värejä korostukseen
            if i == index:
                # Yritetään saada selkeä korostusväri
                hover_color = btn._apply_appearance_mode(ctk.ThemeManager.theme["CTkButton"]["hover_color"])
                text_color_selected = btn._apply_appearance_mode(ctk.ThemeManager.theme["CTkButton"]["text_color_disabled"]) # Tai jokin muu kontrasti
                btn.configure(fg_color=hover_color, text_color=text_color_selected)
            else:
                # Palautetaan oletusarvoihin (läpinäkyvä tausta, normaali tekstin väri)
                default_text_color = btn._apply_appearance_mode(ctk.ThemeManager.theme["CTkLabel"]["text_color"])
                btn.configure(fg_color="transparent", text_color=default_text_color)


    def _populate_scrollable_list(self):
        for widget in self.scrollable_list_frame.winfo_children():
            widget.destroy()
        self.list_item_widgets.clear()
        current_selection_value = self.selected_index_var.get() 
        self.selected_index_var.set(-1) # Nollaa valinta väliaikaisesti

        for i, step in enumerate(self.test_order):
            display_name = step.get('name', f"{AVAILABLE_TEST_TYPES.get(step['type'], step['type'].capitalize())} (ID: {step['id'][:4]})")
            
            item_frame = ctk.CTkFrame(self.scrollable_list_frame, fg_color="transparent")
            item_frame.pack(fill="x", pady=1, padx=1)
            
            # Luodaan lambda niin, että se sitoo oikean 'i':n arvon komennolle
            # Tämä on yleinen tapa Pythonissa luoda callbackeja silmukassa
            def create_command_for_index(idx):
                return lambda: self._select_item(idx)

            item_button = ctk.CTkButton(item_frame, text=f"{i+1}. {display_name}", 
                                       anchor="w", 
                                       fg_color="transparent", 
                                       hover=False, # Otetaan oletushover pois, koska hoidamme sen itse
                                       text_color=ctk.ThemeManager.theme["CTkLabel"]["text_color"], # Käytä labelin oletusväriä
                                       command=create_command_for_index(i)) 
            item_button.pack(fill="x", expand=True)
            self.list_item_widgets.append((item_frame, item_button))
        
        # Palauta valinta, jos se oli olemassa ja on edelleen validi
        if 0 <= current_selection_value < len(self.list_item_widgets):
            self._select_item(current_selection_value)

    def _get_selected_index(self) -> Optional[int]:
        idx = self.selected_index_var.get()
        if 0 <= idx < len(self.test_order):
            return idx
        else:
            # Ei näytetä messageboxia tässä, koska se voi olla ärsyttävää, jos käyttäjä ei ole vielä valinnut mitään.
            # Toiminnot (move_up, jne.) voivat näyttää sen tarvittaessa.
            return None

    def _move_up(self):
        idx = self._get_selected_index()
        if idx is None:
            messagebox.showwarning("Ei valintaa", "Valitse ensin vaihe listasta.", parent=self)
            return
        if idx > 0:
            self.test_order[idx], self.test_order[idx-1] = self.test_order[idx-1], self.test_order[idx]
            self.selected_index_var.set(idx - 1) 
            self._populate_scrollable_list()

    def _move_down(self):
        idx = self._get_selected_index()
        if idx is None:
            messagebox.showwarning("Ei valintaa", "Valitse ensin vaihe listasta.", parent=self)
            return
        if idx < len(self.test_order) - 1:
            self.test_order[idx], self.test_order[idx+1] = self.test_order[idx+1], self.test_order[idx]
            self.selected_index_var.set(idx + 1) 
            self._populate_scrollable_list()

    def _remove_selected(self):
        idx = self._get_selected_index()
        if idx is None:
            messagebox.showwarning("Ei valintaa", "Valitse ensin vaihe listasta.", parent=self)
            return
        del self.test_order[idx]
        self.selected_index_var.set(-1) # Nollaa valinta poiston jälkeen
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

        new_step = {'id': step_id, 'type': test_type, 'name': display_name}
        self.test_order.append(new_step)
        self.selected_index_var.set(len(self.test_order) - 1) 
        self._populate_scrollable_list()
        self.new_test_name_var.set("") # Tyhjennä nimikenttä

    def _on_ok(self):
        self.result_test_order = self.test_order
        self.destroy()

    def _on_cancel(self):
        self.result_test_order = None
        self.destroy()

    def get_test_order(self) -> Optional[List[Dict[str, str]]]:
        return self.result_test_order