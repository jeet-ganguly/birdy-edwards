import time
import pickle
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

options = Options()
#options.add_argument("--headless=new")
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_experimental_option("excludeSwitches", ["enable-automation"])

driver = webdriver.Chrome(options=options)
driver.get("https://www.facebook.com")

print("Login manually in the browser...")
time.sleep(60)   # give time to login

# Save cookies
pickle.dump(driver.get_cookies(), open("fb_cookies.pkl", "wb"))

print("Cookies saved!")

driver.quit()