import os
from fastapi import FastAPI, Request, HTTPException, Body
from fastapi.responses import RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import asyncpg
import urllib.parse
import requests
from datetime import datetime, timedelta, timezone
from celery import Celery
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import asyncio
import time
import json

load_dotenv()

GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
GOOGLE_REDIRECT_URI = os.getenv("GOOGLE_REDIRECT_URI")
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")

SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/userinfo.email"
]

app = FastAPI()

# Allow frontend dev access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Utility: Get DB connection
async def get_db():
    conn = await asyncpg.connect(SUPABASE_DB_URL)
    try:
        yield conn
    finally:
        await conn.close()

# Utility: Ensure user exists in users table with joined_at
async def ensure_user(user_id):
    conn = await asyncpg.connect(SUPABASE_DB_URL)
    await conn.execute(
        """
        INSERT INTO users (id, joined_at)
        VALUES ($1, NOW())
        ON CONFLICT (id) DO NOTHING
        """,
        user_id
    )
    await conn.close()

# Utility: Get user's created_at from auth.users
async def get_user_created_at(user_id):
    conn = await asyncpg.connect(SUPABASE_DB_URL)
    row = await conn.fetchrow("SELECT created_at FROM auth.users WHERE id = $1", user_id)
    await conn.close()
    return row['created_at'] if row else None

@app.get("/auth/login")
async def gmail_login(user_id: str):
    # user_id: Supabase user id (should be passed from frontend)
    params = {
        "client_id": GOOGLE_CLIENT_ID,
        "redirect_uri": GOOGLE_REDIRECT_URI,
        "response_type": "code",
        "scope": " ".join(SCOPES),
        "access_type": "offline",
        "prompt": "consent",
        "state": user_id
    }
    url = f"https://accounts.google.com/o/oauth2/v2/auth?{urllib.parse.urlencode(params)}"
    return RedirectResponse(url)

# In /auth/callback, after storing tokens, ensure user exists
@app.get("/auth/callback")
async def gmail_callback(request: Request):
    code = request.query_params.get("code")
    state = request.query_params.get("state")  # user_id
    if not code or not state:
        raise HTTPException(status_code=400, detail="Missing code or state")
    # Exchange code for tokens
    token_url = "https://oauth2.googleapis.com/token"
    data = {
        "code": code,
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "redirect_uri": GOOGLE_REDIRECT_URI,
        "grant_type": "authorization_code"
    }
    resp = requests.post(token_url, data=data)
    if not resp.ok:
        raise HTTPException(status_code=400, detail="Failed to get tokens from Google")
    tokens = resp.json()
    access_token = tokens["access_token"]
    refresh_token = tokens.get("refresh_token")
    expires_in = tokens["expires_in"]
    token_expiry = datetime.now(timezone.utc) + timedelta(seconds=expires_in)
    user_id = state
    # Store tokens in Supabase
    async for conn in get_db():
        await conn.execute(
            """
            insert into gmail_tokens (user_id, access_token, refresh_token, token_expiry)
            values ($1, $2, $3, $4)
            on conflict (user_id) do update set access_token = $2, refresh_token = $3, token_expiry = $4
            """,
            user_id, access_token, refresh_token, token_expiry
        )
    # Ensure user exists in users table
    await ensure_user(user_id)
    return {"msg": "Gmail connected! You can close this window."}

CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
celery_app = Celery("tasks", broker=CELERY_BROKER_URL)

# Celery Beat schedule: run fetch_and_store_emails every 5 minutes
celery_app.conf.beat_schedule = {
    'fetch-emails-every-minute': {
        'task': 'main.fetch_and_store_emails',
        'schedule': 60.0,  # every minute
    },
}

# Helper: Fetch all users with Gmail tokens
def get_all_gmail_users():
    async def _get():
        conn = await asyncpg.connect(SUPABASE_DB_URL)
        rows = await conn.fetch("SELECT user_id, access_token, refresh_token, token_expiry FROM gmail_tokens")
        await conn.close()
        return rows
    return asyncio.run(_get())

# Helper: Store email in DB
def store_email(email, user_id):
    async def _store():
        conn = await asyncpg.connect(SUPABASE_DB_URL)
        await conn.execute(
            """
            INSERT INTO emails (email_id, user_id, sender, subject, body_raw, received_at, read, summary, category)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (email_id, user_id) DO UPDATE
            SET summary = EXCLUDED.summary, category = EXCLUDED.category
            """,
            email['id'], user_id, email['sender'], email['subject'], email['body'], email['received_at'], False, email.get('summary'), email.get('category')
        )
        await conn.close()
    asyncio.run(_store())

# Helper: Parse Gmail message
def parse_gmail_message(msg):
    headers = {h['name']: h['value'] for h in msg['payload'].get('headers', [])}
    sender = headers.get('From', '')
    subject = headers.get('Subject', '')
    body = ''
    if 'parts' in msg['payload']:
        for part in msg['payload']['parts']:
            if part['mimeType'] == 'text/plain':
                body = part['body'].get('data', '')
                break
    else:
        body = msg['payload']['body'].get('data', '')
    try:
        body = base64.urlsafe_b64decode(body).decode('utf-8')
    except Exception:
        try:
            body = quopri.decodestring(body).decode('utf-8')
        except Exception:
            pass
    received_at = msg['internalDate']
    # Always return a timezone-aware UTC datetime
    received_at = datetime.fromtimestamp(int(received_at)//1000, tz=timezone.utc)
    return {
        'id': msg['id'],
        'sender': sender,
        'subject': subject,
        'body': body,
        'received_at': received_at
    }

# Helper: Set AI disabled flag for user
async def set_ai_disabled(user_id, reason):
    conn = await asyncpg.connect(SUPABASE_DB_URL)
    await conn.execute(
        """
        INSERT INTO ai_status (user_id, ai_disabled, reason, updated_at)
        VALUES ($1, TRUE, $2, NOW())
        ON CONFLICT (user_id) DO UPDATE SET ai_disabled = TRUE, reason = $2, updated_at = NOW()
        """,
        user_id, reason
    )
    await conn.close()

# Helper: Check if AI is disabled for user
async def is_ai_disabled(user_id):
    conn = await asyncpg.connect(SUPABASE_DB_URL)
    row = await conn.fetchrow("SELECT ai_disabled, reason FROM ai_status WHERE user_id = $1", user_id)
    await conn.close()
    return row['ai_disabled'] if row else False, row['reason'] if row else None

# In fetch_and_store_emails, only process emails with received_at >= created_at
@celery_app.task(bind=True, max_retries=None)
def fetch_and_store_emails(self):
    users = get_all_gmail_users()
    print(f"[Agent] Running fetch_and_store_emails for {len(users)} users...")
    for user in users:
        # Check if AI is disabled for this user
        ai_disabled, reason = asyncio.run(is_ai_disabled(user['user_id']))
        if ai_disabled:
            print(f"[Agent] Skipping user {user['user_id']} - AI disabled: {reason}")
            continue
        created_at = asyncio.run(get_user_created_at(user['user_id']))
        # Ensure created_at is timezone-aware (UTC)
        if created_at is not None and created_at.tzinfo is None:
            from datetime import timezone
            created_at = created_at.replace(tzinfo=timezone.utc)
        print(f"[Agent][DEBUG] User {user['user_id']} created_at: {created_at}")
        creds = Credentials(
            token=user['access_token'],
            refresh_token=user['refresh_token'],
            token_uri='https://oauth2.googleapis.com/token',
            client_id=GOOGLE_CLIENT_ID,
            client_secret=GOOGLE_CLIENT_SECRET
        )
        service = build('gmail', 'v1', credentials=creds)
        after_date = created_at.strftime('%Y/%m/%d')
        query = f'is:unread after:{after_date}'
        messages = []
        next_page_token = None
        while True:
            params = {'userId': 'me', 'labelIds': ['INBOX'], 'q': query}
            if next_page_token:
                params['pageToken'] = next_page_token
            results = service.users().messages().list(**params).execute()
            messages.extend(results.get('messages', []))
            next_page_token = results.get('nextPageToken')
            if not next_page_token:
                break
        print(f"[Agent] User {user['user_id']} - Found {len(messages)} unread emails (since {created_at})")
        processed_count = 0
        for m in messages:
            retry_start = time.time()
            while True:
                try:
                    # 1. Retrieve the email
                    msg = service.users().messages().get(userId='me', id=m['id'], format='full').execute()
                    parsed = parse_gmail_message(msg)
                    print(f"[Agent][STEP 1] Retrieved email {parsed['id']}")
                    # 2. Record the email timestamp
                    if parsed['received_at'].tzinfo is None:
                        parsed['received_at'] = parsed['received_at'].replace(tzinfo=timezone.utc)
                    print(f"[Agent][STEP 2] Email {parsed['id']} timestamp: {parsed['received_at']}")
                    # 3. Record the email sender
                    print(f"[Agent][STEP 3] Email {parsed['id']} sender: {parsed['sender']}")
                    if created_at.tzinfo is None:
                        created_at = created_at.replace(tzinfo=timezone.utc)
                    if parsed['received_at'] < created_at:
                        break
                    # 4. Summarize the email body
                    gemini_url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"
                    headers = {
                        "Content-Type": "application/json",
                        "X-goog-api-key": GEMINI_API_KEY,
                    }
                    payload = {"contents": [{"parts": [{"text": f'''Summarize this email:\n{parsed['body']}'''}]}]}
                    resp = requests.post(gemini_url, headers=headers, json=payload)
                    if resp.ok:
                        summary = resp.json()["candidates"][0]["content"]["parts"][0]["text"]
                        print(f"[Agent][STEP 4] Email {parsed['id']} summary: {summary}")
                    else:
                        # Check for specific error codes
                        try:
                            err_json = resp.json()
                            err_msg = err_json.get('error', {}).get('message', '')
                            err_code = resp.status_code
                        except Exception:
                            err_msg = resp.text
                            err_code = resp.status_code
                        if err_code == 429 or (500 <= err_code < 600):
                            # Retry on rate limit or server error
                            if time.time() - retry_start > 60:
                                print(f"[Agent][ERROR] Too many retries for email {parsed['id']}, pausing fetching for 5 minutes.")
                                time.sleep(300)
                                break
                            print(f"[Agent][RETRY] Gemini error {err_code}: {err_msg}. Retrying...")
                            time.sleep(5)
                            continue
                        elif 'token' in err_msg.lower() and 'expire' in err_msg.lower():                
                            asyncio.run(set_ai_disabled(user['user_id'], 'Token expired'))
                            print(f"[Agent][DISABLE] User {user['user_id']} - AI disabled due to token expired.")
                            break
                        elif 'quota' in err_msg.lower() or 'credit' in err_msg.lower() or 'pricing' in err_msg.lower():
                            asyncio.run(set_ai_disabled(user['user_id'], 'Quota or pricing issue'))
                            print(f"[Agent][DISABLE] User {user['user_id']} - AI disabled due to quota/pricing.")
                            break
                        else:
                            print(f"[Agent][ERROR] Gemini error {err_code}: {err_msg}. Not retrying.")
                            break
                    # 5. Categorize using the summary
                    cat_prompt = (
                        "Categorize this email as one of: work, job_application, social, promotion_other, security. "
                        "Only reply with the category.\nSummary:\n" + summary
                    )
                    payload = {"contents": [{"parts": [{"text": cat_prompt}]}]}
                    resp = requests.post(gemini_url, headers=headers, json=payload)
                    if resp.ok:
                        category = resp.json()["candidates"][0]["content"]["parts"][0]["text"].strip().lower()
                        print(f"[Agent][STEP 5] Email {parsed['id']} category: {category}")
                    else:
                        # Check for specific error codes
                        try:
                            err_json = resp.json()
                            err_msg = err_json.get('error', {}).get('message', '')
                            err_code = resp.status_code
                        except Exception:
                            err_msg = resp.text
                            err_code = resp.status_code
                        if err_code == 429 or (500 <= err_code < 600):
                            if time.time() - retry_start > 60:
                                print(f"[Agent][ERROR] Too many retries for email {parsed['id']}, pausing fetching for 5 minutes.")
                                time.sleep(300)
                                break
                            print(f"[Agent][RETRY] Gemini error {err_code}: {err_msg}. Retrying...")
                            time.sleep(5)
                            continue
                        elif 'token' in err_msg.lower() and 'expire' in err_msg.lower():
                            asyncio.run(set_ai_disabled(user['user_id'], 'Token expired'))
                            print(f"[Agent][DISABLE] User {user['user_id']} - AI disabled due to token expired.")
                            break
                        elif 'quota' in err_msg.lower() or 'credit' in err_msg.lower() or 'pricing' in err_msg.lower():
                            asyncio.run(set_ai_disabled(user['user_id'], 'Quota or pricing issue'))
                            print(f"[Agent][DISABLE] User {user['user_id']} - AI disabled due to quota/pricing.")
                            break
                        else:
                            print(f"[Agent][ERROR] Gemini error {err_code}: {err_msg}. Not retrying.")
                            break
                    # 6. Store email in DB (with summary and category)
                    email_to_store = parsed.copy()
                    email_to_store['summary'] = summary
                    email_to_store['category'] = category
                    asyncio.run(store_email(email_to_store, user['user_id']))
                    processed_count += 1
                    break  # Success, move to next email
                except Exception as e:
                    print(f"[Agent][ERROR] Exception: {e}")
                    if time.time() - retry_start > 60:
                        print(f"[Agent][ERROR] Too many retries for email {parsed['id']}, pausing fetching for 5 minutes.")
                        time.sleep(300)
                        break
                    time.sleep(5)
                    continue
        print(f"[Agent] User {user['user_id']} - Processed {processed_count} new/unprocessed emails.")

# Manual trigger endpoint (for dev/testing)
@app.post("/trigger-fetch")
async def trigger_fetch():
    fetch_and_store_emails.delay()
    return {"msg": "Fetch task triggered"}

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

def parse_command_with_gemini(command: str) -> dict:
    """Send the user's command to Gemini to extract intent and parameters as JSON."""
    gemini_url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"
    headers = {
        "Content-Type": "application/json",
        "X-goog-api-key": GEMINI_API_KEY,
    }
    prompt = f"""
You are an expert email assistant. Parse the following user command and return a JSON object with these fields:
- intent: one of [summarize, unread, categorize, filter, unknown]
- filters: any relevant filters (e.g. time, sender, category, keywords)
- details: any extra info

Examples:
Command: Summarize all work emails from yesterday
{{"intent": "summarize", "filters": {{"category": "work", "time": "yesterday"}}, "details": {{}}}}

Command: List unread emails from John
{{"intent": "unread", "filters": {{"sender": "John"}}, "details": {{}}}}

Command: Categorize my emails from today
{{"intent": "categorize", "filters": {{"time": "today"}}, "details": {{}}}}

Command: Show me emails about invoices
{{"intent": "filter", "filters": {{"keyword": "invoices"}}, "details": {{}}}}

Command: {command}

Respond ONLY with a JSON object, no explanation.
"""
    payload = {"contents": [{"parts": [{"text": prompt}]}]}
    resp = requests.post(gemini_url, headers=headers, json=payload)
    if not resp.ok:
        return {"intent": "unknown", "filters": {}, "details": {}}
    try:
        text = resp.json()["candidates"][0]["content"]["parts"][0]["text"]
        # Find the first { ... } JSON object in the response
        start = text.find('{')
        end = text.rfind('}') + 1
        if start == -1 or end == -1:
            return {"intent": "unknown", "filters": {}, "details": {}}
        parsed = json.loads(text[start:end])
        return parsed
    except Exception:
        return {"intent": "unknown", "filters": {}, "details": {}}

# Example AI endpoint (summarize-email):
@app.post("/summarize-email")
async def summarize_email(email_id: str = Body(...), user_id: str = Body(...)):
    ai_disabled, reason = await is_ai_disabled(user_id)
    if ai_disabled:
        return {"error": f"AI features are disabled: {reason}. Please renew your token or resolve your quota issue."}
    # Fetch email body from Supabase
    async for conn in get_db():
        row = await conn.fetchrow(
            "SELECT body_raw FROM emails WHERE email_id = $1 AND user_id = $2",
            email_id, user_id
        )
        if not row:
            raise HTTPException(status_code=404, detail="Email not found")
        body = row["body_raw"]
    # Call Gemini API for summarization
    gemini_url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"
    headers = {
        "Content-Type": "application/json",
        "X-goog-api-key": GEMINI_API_KEY,
    }
    payload = {
        "contents": [{"parts": [{"text": f'''Summarize this email:\n{body}'''}]}]
    }
    resp = requests.post(gemini_url, headers=headers, json=payload)
    if not resp.ok:
        raise HTTPException(status_code=500, detail="Gemini API error")
    summary = resp.json()["candidates"][0]["content"]["parts"][0]["text"]
    # Store summary in Supabase
    async for conn in get_db():
        await conn.execute(
            "UPDATE emails SET summary = $1 WHERE email_id = $2 AND user_id = $3",
            summary, email_id, user_id
        )
    return {"summary": summary}

def get_time_filter_range(time_filter):
    now = datetime.now(timezone.utc)
    if time_filter == "today":
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        return start, now
    elif time_filter == "yesterday":
        start = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        end = (now - timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=999999)
        return start, end
    elif time_filter == "this_week":
        start = now - timedelta(days=now.weekday())
        start = start.replace(hour=0, minute=0, second=0, microsecond=0)
        return start, now
    elif time_filter == "last_week":
        start = now - timedelta(days=now.weekday() + 7)
        start = start.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=6, hours=23, minutes=59, seconds=59, microseconds=999999)
        return start, end
    else:
        return None, None

# Example AI endpoint (agent-command):
@app.post("/agent-command")
async def agent_command(command: str = Body(...), user_id: str = Body(...)):
    print(f"[Agent][STEP 0] Received command: {command} for user: {user_id}")
    ai_disabled, reason = await is_ai_disabled(user_id)
    if ai_disabled:
        print(f"[Agent][STEP 0.1] AI disabled for user: {user_id} - {reason}")
        return {"error": f"AI features are disabled: {reason}. Please renew your token or resolve your quota issue."}
    created_at = await get_user_created_at(user_id)
    print(f"[Agent][STEP 1] User created_at: {created_at}")
    parsed = parse_command_with_gemini(command)
    print(f"[Agent][STEP 2] LLM parsed: {parsed}")
    intent = parsed.get("intent", "unknown")
    filters = parsed.get("filters", {})
    details = parsed.get("details", {})
    # Python fallback: if LLM fails, extract likely category/keyword/time
    if intent == "unknown" or not filters:
        cmd_lower = command.lower()
        if "work" in cmd_lower:
            filters["category"] = "work"
            if "summarize" in cmd_lower:
                intent = "summarize"
        elif "summarize" in cmd_lower:
            intent = "summarize"
        elif "unread" in cmd_lower:
            intent = "unread"
        elif "categorize" in cmd_lower or "category" in cmd_lower:
            intent = "categorize"
        # Time fallback
        if "today" in cmd_lower:
            filters["time"] = "today"
        elif "yesterday" in cmd_lower:
            filters["time"] = "yesterday"
        elif "this week" in cmd_lower:
            filters["time"] = "this_week"
        elif "last week" in cmd_lower:
            filters["time"] = "last_week"
        else:
            filters["time"] = "all"
    time_filter = filters.get("time", "all")
    print(f"[Agent][STEP 3] Time filter extracted: {time_filter}")
    start, end = get_time_filter_range(time_filter)
    print(f"[Agent][STEP 4] Date range: start={start}, end={end}")
    async for conn in get_db():
        if start and end:
            rows = await conn.fetch(
                "SELECT * FROM emails WHERE user_id = $1 AND received_at >= $2 AND received_at <= $3 AND received_at >= $4",
                user_id, start, end, created_at
            )
        else:
            rows = await conn.fetch(
                "SELECT * FROM emails WHERE user_id = $1 AND received_at >= $2",
                user_id, created_at
            )
    print(f"[Agent][STEP 5] Emails found in DB for time period: {len(rows)}")
    # Always filter by category from DB, not just LLM guess
    filtered_rows = rows
    category = filters.get("category")
    if category:
        filtered_rows = [row for row in filtered_rows if (row.get('category') or '').lower() == category.lower()]
        print(f"[Agent][STEP 6] Emails after category filter '{category}': {len(filtered_rows)}")
    else:
        print(f"[Agent][STEP 6] No category filter applied.")
    if intent == "summarize":
        if not filtered_rows:
            print(f"[Agent][STEP 7] No emails found for intent '{intent}' and category '{category}' in time period.")
            return {"response": f"There was no {category or intent} email for {time_filter.replace('_', ' ')}."}
        # Build a prompt for Gemini to summarize all emails as a single conversational update
        email_texts = ""
        for row in filtered_rows:
            subj = row.get('subject') or "(No Subject)"
            body = row.get('body_raw') or ""
            email_texts += f"Subject: {subj}\nBody: {body[:500]}\n\n"  # Truncate body for length
        # Always determine the date range for the summary
        if start and end:
            dates = [row.get('received_at').date() for row in filtered_rows if row.get('received_at')]
            unique_dates = set(dates)
            if len(unique_dates) == 1:
                only_date = unique_dates.pop().strftime('%Y-%m-%d')
                date_range_str = f"Summarize {category or 'these'} emails from {only_date}.\nBegin your summary with the date and category, e.g., 'On {only_date}, you received a {category or 'email'}: ...'."
            else:
                date_range_str = f"Summarize {category or 'these'} emails from {start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}.\nBegin your summary with the date range and category, e.g., 'From {start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}, you received {category or 'emails'}: ...'."
        elif start:
            date_range_str = f"Summarize {category or 'these'} emails from {start.strftime('%Y-%m-%d')} onwards.\nBegin your summary with the date and category, e.g., 'On {start.strftime('%Y-%m-%d')}, you received a {category or 'email'}: ...'."
        else:
            if filtered_rows:
                dates = [row.get('received_at').date() for row in filtered_rows if row.get('received_at')]
                unique_dates = set(dates)
                if len(unique_dates) == 1:
                    only_date = unique_dates.pop().strftime('%Y-%m-%d')
                    date_range_str = f"Summarize all {category or ''} emails from {only_date}.\nBegin your summary with the date and category, e.g., 'On {only_date}, you received a {category or 'email'}: ...'."
                elif dates:
                    min_date = min(dates).strftime('%Y-%m-%d')
                    max_date = max(dates).strftime('%Y-%m-%d')
                    date_range_str = f"Summarize all {category or ''} emails from {min_date} to {max_date}.\nBegin your summary with the date range and category, e.g., 'From {min_date} to {max_date}, you received {category or 'emails'}: ...'."
                else:
                    date_range_str = f"Summarize all {category or ''} emails.\nBegin your summary with the date(s) and category if possible."
            else:
                date_range_str = f"Summarize all {category or ''} emails.\nBegin your summary with the date(s) and category if possible."
        summary_prompt = f"""
You are an executive assistant. {date_range_str} Summarize the following emails as a single, high-level, conversational update for your boss. Focus on the most important points, trends, and any actions needed. Be concise and professional. If you refer to the time period, use the explicit dates provided, not vague terms like 'yesterday' or 'last week'.

Emails:
{email_texts}

Summary:
"""
        print(f"[Agent][STEP 8] Sending to Gemini:\n{summary_prompt[:500]}")
        gemini_url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"
        headers = {
            "Content-Type": "application/json",
            "X-goog-api-key": GEMINI_API_KEY,
        }
        payload = {"contents": [{"parts": [{"text": summary_prompt}]}]}
        resp = requests.post(gemini_url, headers=headers, json=payload)
        print(f"[Agent][STEP 9] Gemini response status: {resp.status_code}")
        print(f"[Agent][STEP 10] Gemini response text: {resp.text[:1000]}")
        if not resp.ok:
            return {"response": "AI summarization failed. Please try again later."}
        try:
            summary = resp.json()["candidates"][0]["content"]["parts"][0]["text"]
        except Exception:
            summary = "AI summarization failed to parse response."
        print(f"[Agent][STEP 11] Final summary: {summary}")
        return {"response": summary}
    elif intent == "unread":
        unread = [row for row in filtered_rows if not row['read']]
        if not unread:
            return {"response": "No unread emails found in this time range."}
        resp = [f"{row['subject'] or '(No Subject)'} from {row['sender']} at {row['received_at']}" for row in unread]
        return {"response": "\n".join(resp)}
    elif intent == "categorize":
        cats = {}
        for row in filtered_rows:
            cat = row.get('category') or 'Uncategorized'
            cats.setdefault(cat, []).append(row)
        if not cats:
            return {"response": "No emails found to categorize in this time range."}
        resp = []
        for cat, emails in cats.items():
            resp.append(f"{cat}: {len(emails)} emails")
        return {"response": "\n".join(resp)}
    # fallback to old logic if LLM fails
    if "summarize" in command.lower():
        summaries = []
        for row in filtered_rows:
            if not row['read']:
                summary = row['summary'] or row['body_raw'][:120] + "..."
                summaries.append(f"{row['subject'] or '(No Subject)'}: {summary}")
        if not summaries:
            return {"response": "No unread emails found to summarize in this time range."}
        return {"response": "\n\n".join(summaries)}
    return {"response": "Sorry, I didn't understand that command yet. Try: 'Summarize all work emails from today'."} 

@app.get("/emails")
async def get_emails(user_id: str):
    """Get all emails for a user from the database."""
    async for conn in get_db():
        rows = await conn.fetch(
            "SELECT * FROM emails WHERE user_id = $1 ORDER BY received_at DESC",
            user_id
        )
    return {"emails": [dict(row) for row in rows]} 

@app.get("/health")
async def health_check():
    """Simple health check endpoint."""
    return {"status": "ok", "message": "Backend is running"} 