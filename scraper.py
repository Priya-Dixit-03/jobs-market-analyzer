from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
import os
import time

load_dotenv()

SEARCH_KEYWORDS = [
    "data engineer",
    "data analyst",
    "python developer",
    "sql developer"
]

def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=os.getenv("DB_PORT"),
        sslmode="require"
    )

def get_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )
    return driver

def scrape_naukri(driver, keyword, pages=2):
    jobs = []

    for page in range(1, pages + 1):
        url = f"https://www.naukri.com/{keyword.replace(' ', '-')}-jobs-{page}"
        print(f"Scraping page {page}: {url}")

        try:
            driver.get(url)
            time.sleep(4)

            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "div.cust-job-tuple, article.jobTuple")
                )
            )

            cards = driver.find_elements(By.CSS_SELECTOR, "div.cust-job-tuple")
            if not cards:
                cards = driver.find_elements(By.CSS_SELECTOR, "article.jobTuple")

            print(f"Found {len(cards)} job cards on page {page}")

            for card in cards:
                try:
                    def safe_find(selector):
                        try:
                            return card.find_element(
                                By.CSS_SELECTOR, selector
                            ).text.strip()
                        except:
                            return None

                    def safe_find_href(selector):
                        try:
                            return card.find_element(
                                By.CSS_SELECTOR, selector
                            ).get_attribute("href")
                        except:
                            return None

                    title     = safe_find("a.title")
                    company   = safe_find("a.comp-name, a.subTitle")
                    location  = safe_find("span.locWdth, li.location")
                    experience= safe_find("span.expwdth, li.experience")
                    salary    = safe_find("span.sal, li.salary")
                    job_url   = safe_find_href("a.title")

                    skills_els = card.find_elements(
                        By.CSS_SELECTOR, "ul.tags-gt li, li.tag-li"
                    )
                    skills = ", ".join(
                        [s.text.strip() for s in skills_els if s.text.strip()]
                    )

                    if title and company:
                        jobs.append({
                            "job_title":    title,
                            "company_name": company,
                            "location":     location,
                            "experience":   experience,
                            "salary":       salary or "Not disclosed",
                            "skills":       skills or None,
                            "job_url":      job_url,
                            "keyword":      keyword
                        })

                except Exception as e:
                    print(f"Card parse error: {e}")
                    continue

        except Exception as e:
            print(f"Page error: {e}")
            continue

        time.sleep(3)

    return jobs

def save_to_db(jobs):
    if not jobs:
        print("No jobs to save")
        return

    conn = get_connection()
    cur = conn.cursor()
    saved = 0

    for job in jobs:
        try:
            cur.execute("""
                INSERT INTO raw_jobs
                    (job_title, company_name, location,
                     experience, salary, skills, job_url)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                job["job_title"],
                job["company_name"],
                job["location"],
                job["experience"],
                job["salary"],
                job["skills"],
                job["job_url"]
            ))
            saved += 1
        except Exception as e:
            print(f"DB insert error: {e}")
            continue

    conn.commit()
    cur.close()
    conn.close()
    print(f"Saved {saved} jobs to database")

def run_scraper():
    print(f"Starting scraper at {datetime.now()}")
    driver = get_driver()
    total_jobs = []

    try:
        for keyword in SEARCH_KEYWORDS:
            print(f"\nSearching: {keyword}")
            jobs = scrape_naukri(driver, keyword, pages=2)
            print(f"Found {len(jobs)} jobs for '{keyword}'")
            total_jobs.extend(jobs)
            time.sleep(3)
    finally:
        driver.quit()

    save_to_db(total_jobs)
    print(f"\nDone! Total jobs scraped: {len(total_jobs)}")

if __name__ == "__main__":
    run_scraper()