import smtplib
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime
from dotenv import load_dotenv
from duckduckgo_search import DDGS
from prefect import flow, task
from prefect.blocks.system import Secret
from prefect.runner.storage import GitRepository

load_dotenv()

@task(name= "Scrape Top News", retries= 3, retry_delay_seconds= 30)
def scrape_top_news(num_results: int = 10) -> list[dict]:
    #Fetches top "num_results" news headlines using DuckDuckGo
    print("Scraping top ", num_results, " news...")

    with DDGS() as ddgs:
        results = list(ddgs.news(
            keywords="top news today",
            region= "wt-wt",
            safesearch= "moderate",
            timelimit= "d",  #last 24 hours
            max_results=num_results
        ))
    
    print("Found ", len(results), " articles")
    return results

@task(name= "Format Email")
def format_email(news_items: list[dict]) -> str:
    #Format news into a clean HTML email
    today = datetime.now().strftime("%A, %B %d, %Y")
    
    html = f"""
    <html>
    <body style="font-family: Arial, sans-serif; max-width: 600px; 
                 margin: 0 auto; padding: 20px;">
        
        <h1 style="color: #2c3e50; border-bottom: 2px solid #3498db; 
                   padding-bottom: 10px;">
            ☕ Your Morning Brief
        </h1>
        <p style="color: #7f8c8d; font-size: 14px;">
            {today} — Top {len(news_items)} stories for you
        </p>
        
        <hr style="border: none; border-top: 1px solid #ecf0f1;">
    """
    
    for i, item in enumerate(news_items, 1):
        title = item.get('title', 'No title')
        url = item.get('url', '#')
        source = item.get('source', 'Unknown source')
        body = item.get('body', '')[:200] + '...' if item.get('body') else ''
        date = item.get('date', '')
        
        html += f"""
        <div style="margin: 20px 0; padding: 15px; 
                    background: #f8f9fa; border-radius: 8px;
                    border-left: 4px solid #3498db;">
            <p style="color: #95a5a6; font-size: 12px; margin: 0 0 5px 0;">
                #{i} · {source} · {date}
            </p>
            <h3 style="margin: 0 0 8px 0;">
                <a href="{url}" style="color: #2c3e50; text-decoration: none;">
                    {title}
                </a>
            </h3>
            <p style="color: #555; font-size: 14px; margin: 0;">
                {body}
            </p>
        </div>
        """
    
    html += """
        <hr style="border: none; border-top: 1px solid #ecf0f1; margin-top: 30px;">
        <p style="color: #bdc3c7; font-size: 12px; text-align: center;">
            Delivered by your Morning Brief Prefect Flow ⚡
        </p>
    </body>
    </html>
    """
    
    return html

@task(name= "Send Email", retries= 2, retry_delay_seconds=60)
def send_email(email_html: str) -> None:
    #Sends the formatted email via Gmail
    """gmail_address = os.getenv("GMAIL_ADDRESS")
    app_password = os.getenv("GMAIL_APP_PASSWORD")
    recipient = os.getenv("RECIPIENT_EMAIL")"""

    gmail_address = Secret.load("gmail-address").get()
    app_password = Secret.load("gmail-app-password").get()
    recipient = Secret.load("recipient-email").get()

    today = datetime.now().strftime("%B %d, %Y")

    msg = MIMEMultipart('alternative')
    msg['Subject'] = f"Here's your Cup of Morning Brief: {today}"
    msg['From'] = gmail_address
    msg['To'] = recipient

    msg.attach(MIMEText(email_html, 'html'))

    print("Sending email...")

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
        server.login(gmail_address, app_password)
        server.sendmail(gmail_address, recipient, msg.as_string())
    
    print("Email sent successfully.")


@flow(name = "Morning Brief", description= "Daily news digest delivered to your inbox every morning")
def morning_brief_flow(num_results: int = 10):
    # Main flow which scrapes news, formats and sends.
    news = scrape_top_news(num_results)
    email_html = format_email(news)
    send_email(email_html)
    print("Morning brief delivered!")

if __name__ == "__main__":
    morning_brief_flow.from_source(
        source="https://github.com/smritikotiyal/morning_brief.git",
        entrypoint="morning_brief.py:morning_brief_flow",
    ).deploy(
        name="morning-brief-deployment",
        work_pool_name="my-managed-pool",
        cron="0 15 * * *",
        tags=["news", "daily", "email"],
        job_variables={
            "pip_packages": [
                "duckduckgo-search>=3.9.0",
                "python-dotenv==1.0.0"
            ]
        }
    )