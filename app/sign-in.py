import time
import pickle
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

#Dear users if you want to manually verify your session is available or not you can use this script

options = Options()
#options.add_argument("--headless=new")
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_experimental_option("excludeSwitches", ["enable-automation"])

driver = webdriver.Chrome(options=options)

driver.get("https://www.facebook.com")

# Load cookies
cookies = pickle.load(open("fb_cookies.pkl", "rb"))

for cookie in cookies:
    driver.add_cookie(cookie)

driver.refresh()

print("Logged in using cookies")

time.sleep(5)

page_source = driver.page_source
current_url = driver.current_url

print(f"\nCurrent URL: {current_url}")

# Check URL
if 'login' in current_url or 'checkpoint' in current_url:
    print("EXPIRED — redirected to login")
else:
    print("URL looks ok")

# Check page signals
logged_out_signals = [
    'id="loginbutton"',
    'name="login"',
    '"isLoggedIn":false',
]

logged_in_signals = [
    '"isLoggedIn":true',
    'c_user',
    '"USER_ID"',
    'id="mount_0_0_',
    'Use another profile',
]

print("\n--- Checking logged-out signals ---")
for sig in logged_out_signals:
    found = sig in page_source
    print(f"  {'❌ FOUND' if found else '✅ not found'}: {sig}")

print("\n--- Checking logged-in signals ---")
for sig in logged_in_signals:
    found = sig in page_source
    print(f"  {'✅ FOUND' if found else '❌ not found'}: {sig}")

driver.quit()