import pyautogui 
import os 
import time 
import subprocess 
import pandas as pd 
from selenium import webdriver 
from selenium.webdriver.chrome.options import Options 
from selenium.webdriver.common.by import By 
from selenium.webdriver.support.ui import WebDriverWait 
from selenium.webdriver.support import expected_conditions as EC 
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException, NoSuchElementException
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup 
import psutil   
from selenium.webdriver.support.ui import Select 
import matplotlib.pyplot as plt 
import logging 
from logging.handlers import RotatingFileHandler 
import random 
import requests 
import re 
import fitz  # PyMuPDF 
import shutil 
import undetected_chromedriver as uc 
import socket
import io
# --- PDF and File Utilities --- 
def extract_cpf(file): 
    try: 
        # Load the PDF 
        doc = fitz.open(file) 

        # Extract text from all pages 
        text = "" 
        for page in doc: 
            text += page.get_text() 

        cnpj = text.split("\n")[2] 
        cpf_match = re.search(r"CPF[:\s]*([\d.-]+)", text) 

        cnpj = re.sub(r'\D', '', cnpj) 
        cpf = re.sub(r'\D', '', cpf_match.group(1)) if cpf_match else None 

        return cnpj, cpf 
    except Exception as e: 
        print(f"Error extracting CPF/CNPJ from PDF: {e}") 
        return None, None 

# Obtain PDF 
def obtain_pdf(driver, wait, period, retries=3, delay=2):
    def try_action(action, description):
        for attempt in range(retries):
            try:
                return action()
            except Exception as e:
                print(f"Attempt {attempt+1} failed for {description}: {e}")
                time.sleep(delay)
        raise Exception(f"Failed to {description} after {retries} attempts.")

    try:
        # Select December checkbox
        def click_checkbox():
            checkbox = wait.until(EC.element_to_be_clickable(
                (By.CSS_SELECTOR, f'input[name="pa"][value$="{period}"]')
            ))
            driver.execute_script("arguments[0].click();", checkbox)
            print(f"✔️ Checkbox selected for {period}")
        try_action(click_checkbox, f"select checkbox for {period}")

        time.sleep(1)

        # Click Apurar / DAS button
        def click_das():
            das_button = wait.until(EC.element_to_be_clickable((By.ID, "btnEmitirDas")))
            driver.execute_script("arguments[0].click();", das_button)
            print("✔️ Apurar / DAS button clicked")
        try_action(click_das, "click Apurar / DAS button")

        time.sleep(2)

        # Click Imprimir/Visualizar PDF
        def click_pdf():
            pdf_link = wait.until(EC.element_to_be_clickable(
                (By.XPATH, '//a[contains(@href, "/pgmei.app/emissao/imprimir")]')
            ))
            driver.execute_script("arguments[0].click();", pdf_link)
            print("✔️ PDF print view opened")
        try:
            try_action(click_pdf, "click PDF print button")
            return True
        except Exception as e:
            print(f"❌ Failed to click PDF print button after retries: {e}")

    except Exception as e:
        print(f"❌ Error obtaining PDF: {e}")
        return False 
def request_pdf(year,pa,session, token_input, data_consolidacao):
    """
    Request the PDF for a given CNPJ and year using the session.
    Returns the PDF content if successful, None otherwise.
    """
    

    url = "https://www8.receita.fazenda.gov.br/SimplesNacional/Aplicacoes/ATSPO/pgmei.app/emissao/gerarDas"
    payload = {
        "ano": year,
        "pa": pa,
        "__RequestVerificationToken": token_input,  # Include the token if required
        "dataConsolidacao": data_consolidacao,  # Include the consolidation date if required
    }
    headers = {
        "Referer": "https://www8.receita.fazenda.gov.br/SimplesNacional/Aplicacoes/ATSPO/pgmei.app/emissao",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/137.0.0.0 Safari/537.36"
        ),
        "X-Requested-With": "XMLHttpRequest",
        "Referer" :  "https://www8.receita.fazenda.gov.br/SimplesNacional/Aplicacoes/ATSPO/pgmei.app/emissao",
    }

    try:
        response = session.post(url, data=payload, headers=headers, timeout=30)
        response.raise_for_status()
        gerarDas_html = response.text
        return gerarDas_html  

    except requests.RequestException as e:
        print(f"Error requesting PDF for  in {year}: {e}")
        return None
def get_cpf_from_pdf(cnpj, session):
    cpf_pattern = re.compile(r"\b\d{3}\.\d{3}\.\d{3}-\d{2}\b")
    cnpj_pattern = re.compile(r"\b\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}\b")
    pdf = session.post("https://www8.receita.fazenda.gov.br/SimplesNacional/Aplicacoes/ATSPO/pgmei.app/emissao/imprimir")
    print(pdf.headers.get("Content-Type")) # application/pdf
    magic = pdf.content[:4]
    print(magic) # b'%PDF' # PDF magic number
    pdf_bytes = pdf.content
    with fitz.open(stream=io.BytesIO(pdf_bytes), filetype="pdf") as doc:
        text = ""
        for page in doc:
            text += page.get_text()
    
    cpf = re.sub(r"\D","",cpf_pattern.findall(text)[0])
    with open(f"../data/pdfs/das_{cnpj}.pdf", "wb") as f:
        f.write(pdf.content)
    obtained_pdf = True
    return cpf, obtained_pdf


# --- Browser/Process Utilities --- 
def autogui_open_page(chrome_profile_path, url, cnpj, port): 
    try: 
        # ---- Step 1: Start Chrome in remote debug mode ---- 
        proc = subprocess.Popen([ 
            r"C:/Program Files/Google/Chrome/Application/chrome.exe", 
            #f"--proxy-server={proxy}", 
            f"--remote-debugging-port={port}", 
            "--user-data-dir=" + chrome_profile_path, 
            "--start-maximized",  # or "--start-fullscreen" 
            "--disable-popup-blocking",  # optional, disable for debugging only 
            "--disable-extensions", 
            "--no-first-run", 
            "--no-default-browser-check" 
        ]) 
        time.sleep(3)  # Give Chrome time to launch 

        # ---- Step 2: Use pyautogui to interact with the site ---- 
        pyautogui.hotkey('ctrl', 'l') 
        pyautogui.typewrite(url, interval=0.01) 
        pyautogui.press('enter')   
        time.sleep(4) 

        #pyautogui.moveTo(x=1027, y=377 , duration=1) # laptop x=671, y=490  deksptop : x=1027, y=377 
        #pyautogui.click() 
        pyautogui.typewrite(cnpj, interval=0.1)
        pyautogui.press('enter')

        #pyautogui.moveTo(x=1027, y=500, duration=1) # Laptop x=675, y=630 desktop: x=1027, y=500 
        #pyautogui.click() 
        time.sleep(2)
        return proc
    except Exception as e: 
        print(f"Error opening page with pyautogui: {e}") 
        return False 
def selenium_open_page(url_inside,port): 

    # Selenium setup 
    options = Options()
    options.add_experimental_option("debuggerAddress", f"127.0.0.1:{port}") 
    driver = webdriver.Chrome(options=options) 
    wait = WebDriverWait(driver, 3) 

    try:
        print("Chromedriver version:", driver.capabilities['chrome']['chromedriverVersion'])
    except Exception as e:
        print(f"Could not get Chromedriver version: {e}")

    driver.get(url_inside) 
    time.sleep(1.5) 
    return driver, wait 
def remove_chrome_profile_dir(path, retries=0, delay=2): 
    for attempt in range(retries): 
        try: 
            if os.path.exists(path):
                print(f"Removing Chrome profile directory: {path}") 
                shutil.rmtree(path, ignore_errors=True) 
            return 
        except Exception as exc:
            e = exc 
            print(f"Retrying removal of {path} due to: {e}") 
            time.sleep(delay)
        if e is not None: 
            print(f"Failed to remove {path} after {retries} attempts: {e}") 
def is_port_available(port, host='127.0.0.1'):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex((host, port)) != 0

# --- Scraping Utilities --- 
def cnpj_check(cnpj,soup): 
    cnpj_check = re.sub(r'\D','',soup.find("li", class_="list-group-item").get_text(strip=True).split("Nome")[0]) 
    # compare the cnpj found on the page with the one we are looking for 
    print(f"cnpj_check: {cnpj_check}") 
    print(f"cnpj: {cnpj}") 
    if cnpj_check != cnpj: 
        raise ValueError(f"CNPJ mismatch: expected {cnpj}, found {cnpj_check}") 
def scrape_data(cnpj, year, soup, table): 
    # Step 1: Extract column headers (excluding unwanted labels) 
    header_rows = table.find('thead').find_all('tr') 
    cols = [] 
    for row in header_rows: 
        headers = [th.get_text(strip=True) for th in row.find_all("th") if th.get_text(strip=True)] 
        filtered = [h for h in headers if h != "Resumo do DAS a ser gerado"] 
        cols.extend(filtered) 
    print(f"Extracted headers: {cols}") 

    # Check if "Quotas" is in the headers to determine if we need to split rows 
    quota_split = "Quotas" in cols 
    quota_index = cols.index("Quotas") if quota_split else None #Find index of "Quotas" column if it exists 

    # Find index of "INSS" column if it exists 
    inss_index = cols.index("Benefício INSS") if "Benefício INSS" in cols else None #Find index of "Benefício INSS" column if it exists 

    # Step 2: Find all relevant data rows 
    rows = soup.find_all("tr", class_="pa") 

    # Step 3: Process data rows with split-row logic 
    cleaned_data = [] 
    i = 0 
    while i < len(rows): 
        row = rows[i] 
        cells = row.find_all("td") 
        
        # Check if the row has INSS box ticked 
        inss_row = any( 
            inp.get("data-benefico-apurado") == "True" 
            for inp in row.find_all("input", attrs={"data-benefico-apurado": True}) 
        ) 
        
        # Extract visible text from the cells (skipping the first <td> with checkbox) 
        cell_texts = [td.get_text(strip=True) for td in cells[1:]] 
        cell_texts[inss_index] = "1" if inss_row else "0" 
        
        if quota_split: #If we have a table with a quotas column 
        
            # Check if each row  has quotas that require a split 
            quota_row = any(
                inp.has_attr("checked")
                for inp in row.find_all("input", class_="quotasSelecionado", attrs={"data-pa-quota": "true"})
            )

            if quota_row: 
                # First 4 cells: Período, Apurado, Benefício, Quotas (set to 1) 
                base_info = cell_texts[:4] 
                base_info[quota_index] = "1" if quota_split else "0" 
                payment_data = cell_texts[4:] 
                cleaned_data.append(base_info + payment_data) 

                # Append next row with same identifying info if exists 
                if i + 1 < len(rows): 
                    next_row = rows[i + 1] 
                    next_cells = next_row.find_all("td") 
                    next_texts = [td.get_text(strip=True) for td in next_cells] 

                    cleaned_data.append(base_info + next_texts) 
                    i += 2 
                else: 
                    i += 1 
            else: 
                # Normal row within a quotas table, treat quotas as 0 if not explicitly set 
                if len(cell_texts) >= 5: 
                    cell_texts[3] = "0" 
                cleaned_data.append(cell_texts) 
                i += 1 

        # Normal table without quotas 
        else:     
            cleaned_data.append(cell_texts) 
            i += 1 

    
    # Step 4: Build DataFrame
    #print(f"Cleaned data : {cleaned_data}")
    df = pd.DataFrame(cleaned_data, columns=cols) 
    df['cnpj'] = cnpj 
    df['data_found'] = "Yes"

    return df 
def debt_collector(soup): 
    # Placeholder for debt collector logic. 
    # if soup contains ATENÇÃO: Existe(m) débitos(s) enviados(s) para inscrição em dívida ativa.then return True 
    attention_text = soup.find(text=re.compile(r"ATENÇÃO: Existe\(m\) débitos\(s\) enviados\(s\) para inscrição em dívida ativa")) 
    if attention_text: 
        return True 
    return False 
def outstanding_payment(data, year): 
    mask = data['Total'].astype(str).str.strip().ne("-") 
    if mask.any(): 
        print("finding first month with outstanding payment")
        first = mask.idxmax() 
        month = first + 1 
        return f"{year}{month:02d}" 
    return None 
def scrape_debt_table(cnpj, soup): 
    all_rows = [] 
    table = soup.find_all("table", class_="table table-bordered table-hover table-condensed") 
    for tbl in table: 
        # Get the period from the caption 
        caption = tbl.find("caption") 
        periodo = caption.get_text(strip=True).replace("Período de Apuração (PA): ", "") if caption else None 

        # Get all rows in tbody 
        for tbody in tbl.find_all("tbody"): 
            for tr in tbody.find_all("tr"): 
                tds = tr.find_all("td") 
                if len(tds) == 4: 
                    tributo = tds[0].get_text(strip=True) 
                    valor = tds[1].get_text(strip=True) 
                    ente = tds[2].get_text(strip=True) 
                    situacao = tds[3].get_text(strip=True) 
                    all_rows.append({ 
                        "Periodo de Apuracao": periodo, 
                        "Tributo": tributo, 
                        "Valor": valor, 
                        "Ente Federado": ente, 
                        "Situacao do Debito": situacao 
                    }) 
    df = pd.DataFrame(all_rows) 
    df["cnpj"] = cnpj 
    return df 

# --- Dropdown and Year Selection --- 
def get_enabled_years_bootstrap(wait, cnpj): 
    """ 
    Try to get enabled years from a Bootstrap-styled dropdown. 
    Returns (enabled_years, opt_out_years,use_bootstrap). 
    Raises if not found. 
    """
    print("Waiting for buttom to be available")
    dropdown_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[data-id="anoCalendarioSelect"]'))) 
    dropdown_button.click() 
    time.sleep(1) 

    year_elements = wait.until(EC.presence_of_all_elements_located( 
        (By.CSS_SELECTOR, ".dropdown-menu.inner li a span.text") 
    )) 
    drop_down_years = [el.text.strip() for el in year_elements if el.text.strip() and el.get_attribute("class") != "disabled"] 
    enabled_years = [year for year in drop_down_years if "Não optante" not in year]
    opt_out_years = [re.search(r"\d{4}", year).group() for year in drop_down_years if "Não optante" in year] 

    if not enabled_years: 
        raise ValueError("No enabled years found in the dropdown menu.") 

    print("Bootstrap dropdown enabled years for CNPJ ", cnpj , ":", enabled_years) 
    return enabled_years, opt_out_years, True 
def get_enabled_years_native(wait, cnpj, driver): 
    """ 
    Get enabled years from a native <select> dropdown. 
    Returns (enabled_years, opt_out_years, use_bootstrap). 
    """ 
    select_element = wait.until(EC.presence_of_element_located((By.ID, "anoCalendarioSelect"))) 
    dropdown = Select(select_element) 
    drop_down_years = [o.text.strip() for o in dropdown.options if o.text.strip()] 
    enabled_years = [year for year in drop_down_years if "Não optante" not in year]
    opt_out_years = [re.search(r"\d{4}", year).group() for year in drop_down_years if "Não optante" in year]
    print("Native <select> enabled years for CNPJ ", cnpj,":", enabled_years) 
    return enabled_years, opt_out_years, False 

    for attempt in range(retries):
        try:
            dropdown = Select(wait.until(
                EC.presence_of_element_located((By.ID, "anoCalendarioSelect"))
            ))
            dropdown.select_by_visible_text(year)
            print(f"Selected (native) year: {year}")
            time.sleep(0.5)
            # Click OK/Submit button if needed
            ok_button = wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button[type='submit']"))
            )
            ok_button.click()
            # ok_button.send_keys(Keys.ENTER)
            time.sleep(2)
            return  # Success
        except (TimeoutException, ElementClickInterceptedException, NoSuchElementException) as e:
            print(f"Attempt {attempt+1} failed to select year {year} (Native): {e}")
            time.sleep(delay)
    raise Exception(f"Failed to select year {year} (Native) after {retries} attempts.")
def get_years(soup):
    enabled_years = []
    opt_out_years = []

    select = soup.find("select", {"id": "anoCalendarioSelect"})
    #print(select)
    if select:
        for option in select.find_all("option"):
            if option.get("value") == "":
                continue
        # Exclude disabled options (Não optante)
            if option.has_attr("disabled"):
                opt_out_years.append(option.text.strip())
                continue
            enabled_years.append(option.text.strip())

    return enabled_years, opt_out_years
def make_requests_session_from_selenium(driver):
    """
    Given a Selenium WebDriver that’s already logged in (and has
    solved any CAPTCHA), extract its cookies into a requests.Session.
    """
    sess = requests.Session()
    for ck in driver.get_cookies():
        # Copy each cookie from Selenium into requests
        sess.cookies.set(
            name   = ck['name'],
            value  = ck['value'],
            domain = ck.get('domain'),
            path   = ck.get('path', '/')
        )
    return sess
def fetch_emissao_html(year: str, session: requests.Session) -> str:
    """
    Given a logged-in session, POST directly to the /emissao endpoint
    and return the HTML response.
    """
    

    url = "https://www8.receita.fazenda.gov.br/SimplesNacional/Aplicacoes/ATSPO/pgmei.app/emissao"
    payload = {
        "ano": year,
        # Include any other hidden form fields you saw (tokens, etc.)
        # "__RequestVerificationToken": session.cookies.get("…"),  # if needed
    }
    headers = {
        "Referer":    "https://www8.receita.fazenda.gov.br/SimplesNacional/Aplicacoes/ATSPO/pgmei.app/emissao",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/114.0.5735.199 Safari/537.36"
        ),
        "X-Requested-With": "XMLHttpRequest",
        "Referer" :  "https://www8.receita.fazenda.gov.br/SimplesNacional/Aplicacoes/ATSPO/pgmei.app/emissao", 
    }

    resp = session.post(url, data=payload, headers=headers, timeout=30)
    resp.raise_for_status()  # explode on HTTP errors
    return resp.text

# --- DataFrame and Data Handling ---   
def handle_missing_table(cnpj, year, enabled_years, index, data, master_df): 

    """ 
    Handles the case when no table is found for a given CNPJ and year. 
    Marks all remaining years as not found and appends to master_df. 
    Returns the updated master_df. 
    """ 
    print(f"No table found for {cnpj} in year {year}. Skipping the rest.") 
    for remaining_year in enabled_years[index:]: 
        data.append({ 
            'cnpj': cnpj, 
            'Período de Apuração': remaining_year, 
            'data_found': "Table not found" 
        }) 
    missing_df = pd.DataFrame(data) 
    master_df = pd.concat([master_df, missing_df], ignore_index=True) 
    return master_df 

# --- Logging/Reporting --- 
def timings_report(start_time, total_start_time,timings): 
    
    end_time = time.time() 
    elapsed = end_time - start_time 
    timings.append(elapsed) 
    total_elapsed = time.time() - total_start_time 
    average_elapsed = sum(timings) / len(timings) 

# --- Queue and docker 
import uuid 
def batch_cnpjs(cnpj_list, batch_size): 
    """Yield successive batches from cnpj_list.""" 
    for i in range(0, len(cnpj_list), batch_size): 
        yield cnpj_list[i:i + batch_size] 
def queue_cnpj_batches(cnpj_list, batch_size=10): 
    from tasks import process_cnpj_batch_task 
    """Queue CNPJ batches as Celery tasks.""" 
    for batch in batch_cnpjs(cnpj_list, batch_size): 
        process_cnpj_batch_task.delay(batch) 
def process_cnpj_batch(chrome_profile_path, cnpj, port): 

    #chrome_profile_path = "C:/Temp/ChromeDebug" 
    url = "https://www8.receita.fazenda.gov.br/SimplesNacional/Aplicacoes/ATSPO/pgmei.app/Identificacao" 
    url_inside = "https://www8.receita.fazenda.gov.br/SimplesNacional/Aplicacoes/ATSPO/pgmei.app/emissao" 
    timings = [] 
    total_start_time = time.time() 
    master_df = pd.DataFrame() 
    master_debt_df = pd.DataFrame()
    cnpj_cpf_map = pd.DataFrame(columns=["cnpj", "cpf"])

    #for cnpj in cnpj_batch: 
    try: 
        start_time = time.time() 
        data = [] 
        
        # Open page and add cookies to selenium
        driver, wait = selenium_open_page(url_inside,port)
        session = make_requests_session_from_selenium(driver)
        
        soup1 = BeautifulSoup(driver.page_source, 'html.parser') 
        cnpj_check(cnpj,soup1) # check cnpj being searched matches the one on the webpage
        enabled_years, opt_out_years = get_years(soup1) # obtain years

        print("scraping years", enabled_years)
        print("opt_out_years", opt_out_years)

        obtained_pdf = False
        for index, year in enumerate(enabled_years): 
            try: 

                html = fetch_emissao_html(year, session) # Get table html    

                time.sleep(2)
                soup = BeautifulSoup(html, 'html.parser')
                # Export soup to a text file with CNPJ in the filename
                os.makedirs("html", exist_ok=True)
                with open(f"html/soup_{cnpj}_{year}.html", "w", encoding="utf-8") as f:
                    f.write(str(soup))
                table = soup.find('table', class_=[
                    'table', 'table-hover', 'table-condensed', 'emissao', 'is-detailed'
                ])

                # Get verification token and data cons for the Payload if we don't have the pdf
                if obtained_pdf == False:
                    try:
                        token_input = soup.find('input', {'name': '__RequestVerificationToken'})
                        if token_input:
                            token_input = token_input.get('value', None)
                        else:
                            token_input = None
                    except Exception as e:
                        token_input = None
                        print(f"__RequestVerificationToken not found for {cnpj} in year {year}: {e}")
                
                    # Get data consolidacao
                    try:
                        data_consolidacao = soup.find('input', {'name': 'dataConsolidacao'})['value']
                    except Exception as e:
                        data_consolidacao = None
                        print(f"dataConsolidacao not found for {cnpj} in year {year}: {e}")

                #print(table)
                if table is not None:
                    print(f"{cnpj} - Table found for {year}")
                if not table:
                    master_df = handle_missing_table(cnpj, year, enabled_years, index, data, master_df) 
                    break 
                
                # Check if the page indicates debt collection
                is_debt_collector = debt_collector(soup)
                print(f"{cnpj} scraping main table for {year}")

                # Scrape the main table
                new_data = scrape_data(cnpj, year, soup, table) 
                master_df = pd.concat([master_df, new_data], ignore_index=True) 
            
                # Scrape the debt collection table if applicable
                if is_debt_collector: 
                    print(f"Debt collection table found in year {year}.") 
                    debt_data = scrape_debt_table(cnpj, soup) 
                    master_debt_df = pd.concat([master_debt_df, debt_data], ignore_index=True) 

                print(f"Type of year: {type(year)}, value: {year}")
                # Check for outstanding payments
                pa = outstanding_payment(new_data, year)
                if pa:
                    print(f"Outstanding payment found for {cnpj} in year {year}: {pa}")
                else:
                    print(f"No outstanding payments found for {cnpj} in year {year}")

                if not is_debt_collector and pa and not obtained_pdf: 
                    print(f"Outstanding payments found in year {year}, period {pa}, attempting to obtain PDF.") 
                    try:
                        print("requesting pdf")
                        request_pdf(year, pa, session, token_input, data_consolidacao)
                        
                        print("obtaining cpf from pdf")
                        cpf, obtained_pdf = get_cpf_from_pdf(cnpj, session)
                        print(f"Obtained CPF: {cpf}")
                        # create dataframe with cnpj and cpf
                        if cpf and not (cnpj_cpf_map['cnpj'] == cnpj).any():
                            cnpj_cpf_map = pd.concat(
                                [cnpj_cpf_map, pd.DataFrame([{"cnpj": cnpj, "cpf": cpf}])],
                                ignore_index=True
                            )
                    except Exception as e:
                        print(f"Error obtaining PDF for {cnpj} in year {year}: {e}")

                
                #driver.back()
                print("back button clicked")
                time.sleep(2) 

            except Exception as e: 
                print(f"Error with year {year}:", e) 

    except Exception as outer_error: 
        print(f"Fatal error with CNPJ {cnpj}:", outer_error) 

    finally: 
        try:
            print("Closing browser...")
            driver.quit() 
        except: 
            pass 
        
        timings_report(start_time, total_start_time, timings) 
   
    # Add indicator in data_found for nao optante years
    if opt_out_years:
        nao_optante_df = pd.DataFrame([{
            'cnpj': cnpj,
            'Período de Apuração': year,
            'data_found': "Nao optante"
        } for year in opt_out_years])
        master_df = pd.concat([master_df, nao_optante_df], ignore_index=True)

    # Add a column to master_df indicating if bootstrap was used
    master_df['obtained_pdf'] =  obtained_pdf

    print(f"removing chrome profile directory: {chrome_profile_path}")
    remove_chrome_profile_dir(chrome_profile_path)
    print("returning datasets")
    print(f"cnpj_cpf_map: {cnpj_cpf_map} ")

    return master_df, master_debt_df, cnpj_cpf_map

def store_data(master_df, master_debt_df, master_mapping):
    # Export dataframes to CSV (optionally, use a unique name per batch) 
    if 'Quotas' in master_df.columns:
        master_df['Quotas'] = master_df['Quotas'].fillna(0).astype(int) 
    master_df["year"] = master_df["Período de Apuração"].str.extract(r'(\d{4})').astype(int) 
    month_mapping = { 
        'janeiro': 1, 'fevereiro': 2, 'março': 3, 'abril': 4, 
        'maio': 5, 'junho': 6, 'julho': 7, 'agosto': 8, 
        'setembro': 9, 'outubro': 10, 'novembro': 11, 'dezembro': 12 
    } 
    master_df["month"] = master_df["Período de Apuração"].str.extract(r'(\w+)') 
    master_df['month'] = master_df['month'].str.lower().map(month_mapping) 
    # Only include 'Quotas' if it exists
    columns = [
        'cnpj', 'Período de Apuração', 'year', 'month', 'Apurado', 'Situação', 'Benefício INSS',
        'Principal', 'Multa', 'Juros', 'Total',
        'Data de Vencimento', 'Data de Acolhimento', 'data_found', "used_bootstrap", "worker_id_port",
        "obtained_pdf"
    ]
    if 'Quotas' in master_df.columns:
        columns.insert(7, 'Quotas')  # Insert 'Quotas' after 'Benefício INSS'

    master_df = master_df[[col for col in columns if col in master_df.columns]] 
    master_df = master_df.sort_values(by=['cnpj', 'year', 'month']) 

    # Save with a unique name per batch (e.g., using time or batch id) 
    batch_id = int(time.time()) 
    master_df.to_csv(f'../data/out/master_df_{batch_id}.csv', index=False, encoding='utf-8') 
    master_debt_df.to_csv(f'../data/out/master_debt_df_{batch_id}.csv', index=False, encoding='utf-8') 
    master_mapping.to_csv(f'../data/out/master_mapping_{batch_id}.csv', index=False, encoding='utf-8')
    print(f"Batch data exported to ../data/master_df_{batch_id}.csv & ../data/master_debt_df_{batch_id}.csv & ../data/master_mapping_{batch_id}.csv") 

