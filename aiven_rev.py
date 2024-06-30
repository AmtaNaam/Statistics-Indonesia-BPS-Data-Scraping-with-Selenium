from selenium import webdriver
#from selenium.webdriver.support import ui
from selenium.webdriver.common.by import By
#from selenium.webdriver.support.ui import Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException


from more_itertools import divide
import pandas as pd

import time
import psycopg2
from sqlalchemy import create_engine
import os


#Initialize our hs code master
hs_master = pd.read_excel("HSCode_Master.xlsx", dtype={'HS Code': str})
hs_master['HS Code'] = hs_master['HS Code'].apply('{:0>8}'.format) #preserve the leading 0


#we need to rename column names for readibility aim
hs_master.rename(columns = {'Description (2022-now)':'Description'}, inplace = True)
hs_master.rename(columns = {'HS Code':'HS_10'}, inplace = True)
#hs_master.rename(columns = {'HS 6':'HS_6'}, inplace = True)


#since, we have more than 1700 products. For convenience and safety purpose, create batches to do a data scraping
hs_10_list = hs_master['HS_10'].tolist()
hs_batches = [list (batch) for batch in divide(87, hs_10_list)] #bps puts 20 products at most

years = list(range(2014, 2024))
# Creating sublists
years_str = [str(year) for year in years]
# Dividing into two sublists
sublist_1 = years_str[:5]  # 2014-2018
sublist_2 = years_str[5:]
years = [sublist_1, sublist_2]


url = 'https://www.bps.go.id/id/exim'

#creating a function to read the last completed batch index
def read_last_completed_batch():
    if os.path.exists("last_completed_batch.txt"):
        with open("last_completed_batch.txt", "r") as file:
            return int(file.read().strip())
    return 0

#next, we need to create a function to save the last completed batch index
def save_last_completed_batch(index):
    with open("last_completed_batch.txt","w") as file:
        file.write(str(index))

def find_element_with_retry(driver, by, value, retries=3, delay=5):
    for attempt in range(retries):
        try:
            return WebDriverWait(driver, delay).until(EC.presence_of_element_located((by, value)))
        except TimeoutException:
            print(f"Percobaan {attempt+1}: Element {value} tidak ditemukan, me-refresh halaman...")
            driver.refresh()
            driver.get(url)
            time.sleep(delay)
    raise TimeoutException(f"Element {value} tidak ditemukan setelah {retries} kali percobaan.")

def process_batch(driver, batch):
    container = find_element_with_retry(driver, By.CLASS_NAME, "w-full")
    data_choice = container.find_element(By.XPATH, '//*[@id="jenis-radio-2"]')
    actions = ActionChains(driver)
    actions.move_to_element(data_choice).click().perform()
    
    container.find_element(By.XPATH, "//input[@id='react-select-filter-agregasi-input']").send_keys("Menurut Kode HS" + Keys.RETURN)
    container.find_element(By.XPATH, '//*[@id="react-select-filter-jenishs-input"]').send_keys("HS Full" + Keys.RETURN)

    input_field = find_element_with_retry(driver, By.XPATH, '/html/body/div[2]/div[2]/div[2]/div[1]/div/div[2]/div[3]/div/div/div/div[1]/div[2]/input')
    input_field.send_keys('2023')
    input_field.send_keys(Keys.RETURN)
    time.sleep(2)
    container.click()

    input_field_hs = find_element_with_retry(driver, By.XPATH, '/html/body/div[2]/div[2]/div[2]/div[1]/div/div[2]/div[4]/div[2]/div/div/div[1]/div[2]/input')
    for hsc in batch:
        input_field_hs.send_keys(hsc)
        time.sleep(4)
        actions.send_keys(Keys.RETURN).perform()

    input_field.send_keys(Keys.BACK_SPACE)
    input_field = find_element_with_retry(driver, By.XPATH, '/html/body/div[2]/div[2]/div[2]/div[1]/div/div[2]/div[3]/div/div/div/div[1]/div[2]/input')
    for yrs in sublist_2:
        input_field.send_keys(yrs)
        input_field.send_keys(Keys.RETURN)

    time.sleep(0.3)
    create_table = find_element_with_retry(driver, By.XPATH, '//*[@id="ss"]/div[2]/div[9]/button')
    driver.execute_script("arguments[0].click();", create_table)
    table_cont = find_element_with_retry(driver, By.XPATH, '//*[@id="ss"]/div[3]/div/div[2]')
    print("Identifying table")

    return table_cont
def process_batch_with_retry(driver, batch, retries=4):
    table_cont = None  # Define table_cont in the function scope

    for attempt in range(retries):
        try:
            container = find_element_with_retry(driver, By.CLASS_NAME, "w-full")
            data_choice = container.find_element(By.XPATH, '//*[@id="jenis-radio-1"]')
            actions = ActionChains(driver)
            actions.move_to_element(data_choice).click().perform()
            
            container.find_element(By.XPATH, "//input[@id='react-select-filter-agregasi-input']").send_keys("Menurut Kode HS" + Keys.RETURN)
            container.find_element(By.XPATH, '//*[@id="react-select-filter-jenishs-input"]').send_keys("HS Full" + Keys.RETURN)

            input_field = find_element_with_retry(driver, By.XPATH, '/html/body/div[2]/div[2]/div[2]/div[1]/div/div[2]/div[3]/div/div/div/div[1]/div[2]/input')
            input_field.send_keys('2023')
            input_field.send_keys(Keys.RETURN)
            time.sleep(2)
            container.click()

            input_field_hs = find_element_with_retry(driver, By.XPATH, '/html/body/div[2]/div[2]/div[2]/div[1]/div/div[2]/div[4]/div[2]/div/div/div[1]/div[2]/input')
            for hsc in batch:
                input_field_hs.send_keys(hsc)
                time.sleep(4)
                actions.send_keys(Keys.RETURN).perform()

            input_field.send_keys(Keys.BACK_SPACE)
            input_field = find_element_with_retry(driver, By.XPATH, '/html/body/div[2]/div[2]/div[2]/div[1]/div/div[2]/div[3]/div/div/div/div[1]/div[2]/input')
            for yrs in sublist_2:
                input_field.send_keys(yrs)
                input_field.send_keys(Keys.RETURN)

            time.sleep(0.3)
            create_table = container.find_element(By.XPATH, '//*[@id="ss"]/div[2]/div[9]/button')
            driver.execute_script("arguments[0].click();", create_table)

            print("Identifying table")
            time.sleep(4)
            table_cont = WebDriverWait(driver, 9).until(EC.presence_of_element_located((By.XPATH, '//*[@id="ss"]/div[3]/div/div[2]')))
            break  # Exit loop if successful
        except Exception:
            print(f"Percobaan {attempt + 1}: Elemen tidak ditemukan, merefresh halaman...")
            driver.refresh()
            driver.get(url)
            time.sleep(5)
    
    if not table_cont:
        raise Exception("Gagal menemukan elemen setelah beberapa percobaan.")
        
    
    return table_cont

#read the last completed batch index
last_completed_batch = read_last_completed_batch()
urutan_batch = last_completed_batch



chrome_options = Options()
chrome_options.add_argument('--headless') 
url = 'https://www.bps.go.id/id/exim'
driver = webdriver.Chrome(options=chrome_options)
driver.get(url)



#BPS puts banner on its website, so we need to close it first
#banner = driver.find_element(By.CLASS_NAME, "swiper-wrapper")

#put an explicit wait (gonna break the script, if selenium can't find element in the given amount of time)
wrapper = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, '/html/body/div[6]/div/div/div/div/div/div/div/div[2]/button')))
wrapper.click()

#time.sleep(4) #put an implicit wait (better to use explicit wait than this one)

for urutan_batch, batch in enumerate(hs_batches[last_completed_batch:], start=last_completed_batch):
#next, after we got it. we look for the container
    print("===========================================================================")
    print(f"Starting batch: {urutan_batch}")
    start_time = time.time()
    table_cont = process_batch_with_retry(driver, batch)  # Capture the return value of process_batch
    
    #table_cont = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="ss"]/div[3]/div/div[2]')))
    pvt_cont = table_cont.find_element(By.CLASS_NAME, 'pvtUi')
    pvt_out = pvt_cont.find_element(By.CLASS_NAME, "pvtOutput")
    tbl = pvt_out.find_element(By.CLASS_NAME, 'pvtTable')
    headers = tbl.find_element(By.TAG_NAME, "thead")
    tbody = tbl.find_element(By.TAG_NAME, "tbody")
    
    #create lists placeholders the headers & datas
    #note that these lists are temporary. Since, we need to aggregate them into our main dataframe after
    headers_col = []
    body_data = []
    tahun = []
    kode_hs = []
    header_rows = headers.find_elements(By.TAG_NAME, "tr")
    prod_yr_header = header_rows[3] #product ,and year is at third position
    months = header_rows[2]
    countries_header = header_rows[0]
    #months = months[1:]

    #create countrie dict (country name and their length)
    cnt_col_spn_dict = {}

    for ctr in countries_header.find_elements(By.CLASS_NAME, 'pvtColLabel'):
        colspan = int(ctr.get_attribute("colspan"))
        cnt_col_spn_dict[ctr.text] = colspan
    #Parse the months
    for month in months.find_elements(By.TAG_NAME, 'th')[1:]: #i did this to remove the first elemen. Sinc, the first element isn't necessary
        headers_col.append(month.text)
    headers_col.append("Total")
    #Next, we need to parse the tr element of tbody. But, we need to exclude the last element, since this element is a total, and it's not necessary

    tr_body_rows = tbody.find_elements(By.TAG_NAME, 'tr')

    #excluding the last one
    tr_elements_except_last = tr_body_rows[:-1]

    #do a test
    """for tr in tr_elements_except_last:
        print(tr.text)"""
    
    row_data = []
    #iterate any value of tr, are there any th?
    print("Parsing pivot table data")
    for tr in tr_elements_except_last:
        th_elements = tr.find_elements(By.TAG_NAME,'th') #find any elements with th tag
        #do they have >2 th?
        if len(th_elements)>=2:
            #get the first th element
            first_th = th_elements[0]

            #get the second th element
            second_th = th_elements[1]

            #obtain the rowspan values, and set the default value into 0 if rowspan data
            rowspan = first_th.get_attribute('rowspan')
            if rowspan is None:
                rowspan=1
            else:
                rowspan = int(rowspan)

            tahun.extend([first_th.text]*rowspan)
            kode_hs.append(second_th.text)

        elif len(th_elements)==1:
            #take the first th (and the only one)
            kode_hs.append(th_elements[0].text)

        
        #next, we need to iterate over td elements inside the tr element
        
        td_elements = tr.find_elements(By.TAG_NAME, 'td')
        row_data = []
        for td in td_elements[:-1]: #don't need the last one (total, we need to go deeper if wanna get the tot for each country)
            #if td is empty, replace with 0. Then, append into row_data list
            td_text = td.text.strip()
            if td_text=="":
                row_data.append(0)
            else:
                #eliminate the coma and replace it's value into float
                td_text = td_text.replace(",","")
                try:
                    td_value = float(td_text)
                    row_data.append(td_value)
                except ValueError:
                    row_data.append(0)
        body_data.append(row_data)

    #print(tahun)
    #print(kode_hs)
    #print("Sukses")

    #adjust the dimension of tahun & kode_hs
    tahun_baru = tahun*len(cnt_col_spn_dict)
    kode_hs_baru = kode_hs*len(cnt_col_spn_dict)

    #parse countries data & 
    dfs = {}
    start_index = 0

    for country, num_cols in cnt_col_spn_dict.items():
        data = []
        if start_index == num_cols:
            end_index = num_cols
        else:
            end_index = start_index + num_cols
        
        for sublist in body_data:
            if start_index == num_cols:
                sliced_data = sublist[num_cols]
            else:
                sliced_data = sublist[start_index:end_index]
            data.append(sliced_data)
        
        if start_index == num_cols:
            df = pd.DataFrame(data, columns=[headers_col[num_cols]])
            start_index = end_index+1
        else:
            df = pd.DataFrame(data, columns=headers_col[start_index:end_index])
            start_index = end_index
        
        dfs[country] = df

    #aggregating the data
    ##Create a new dataframe container
    kolom = ['Country', '[01] Januari', '[02] Februari', '[03] Maret', '[04] April',
       '[05] Mei', '[06] Juni', '[07] Juli', '[08] Agustus', '[09] September',
       '[10] Oktober', '[11] November', '[12] Desember']

    df_kosong = pd.DataFrame(columns=kolom)
    #df_kosong.columns

    print("Aggregating data")
    for cnt, data in dfs.items():
        # create a copy to prevent direct modification of dfs data key
        data_copy = data.copy()

        # add country column into copied data
        data_copy.insert(0, "Country", cnt)

        # Do the aggregation
        df_single_aggregated = data_copy.T.groupby(data_copy.columns).sum().T

        # initiate the data_single inside the loop to prevent carrying prevs. iteration data
        data_single = pd.DataFrame(columns=kolom)
        data_single = pd.concat([data_single, df_single_aggregated], ignore_index=True)

        # Add data_single into df_kosong
        df_kosong = pd.concat([df_kosong, data_single], ignore_index=True)

    #df = pd.DataFrame(body_data, columns=headers_col)
    df_kosong.insert(0, 'Tahun', tahun_baru)
    df_kosong.insert(0, 'Kode_HS', kode_hs_baru)
    #creating pysql connector
    #I use pysql to save my data, since this is my bes sql dialect most

    #Adjust to yours
    db_config = { 
        'dbname':'your_db_name',
        'user':'your_username',
        'password':'your_password',
        'host':'your_db_host',
        'port':'the_port'
    }



    #creating url connection
    connection_string = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"

    #create SQLAlchemy engine
    engine = create_engine(connection_string)
    query = "SELECT * FROM data_ekspor_cnt_2 LIMIT 0"
    df_postgre = pd.read_sql(query, engine)
    kolom_postgre = df_postgre.columns.tolist()


    # Menyesuaikan urutan kolom DataFrame sesuai dengan urutan kolom di PostgreSQL
    #df = df[kolom_postgre[1:]]  # Kecuali kolom 'id' karena itu primary key yang otomatis increment di PostgreSQL

    # Menyimpan DataFrame ke tabel PostgreSQL
    #df_grouped.to_sql('data_impor', engine, if_exists='append', index=False)
    try:
        df_kosong.to_sql("data_ekspor_cnt_2", engine, if_exists='append', index=False)
        print("Data berhasil disimpan.")
    except Exception as error:
        print(f"Terjadi kesalahan: {error}")

    
    end_time = time.time()
    elapsed_time = end_time-start_time
    print(f"Batch ke- {urutan_batch} Selesai")
    urutan_batch+=1
    save_last_completed_batch(urutan_batch)
    print(f"Selesai dalam {round(elapsed_time/60, 2) } menit")
    print("===========================================================================")

    driver.refresh()
    driver.get(url)
    
    if urutan_batch >86:
        print("Yay bot sudah menyelesaikan semua batch")
        print("===========================================================================")
        break
    else:
        continue
    
print("Scraping telah selesai !!!!")