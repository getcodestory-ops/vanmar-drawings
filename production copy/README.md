# Procore PDF Merger & Auto-Archiver

A full-stack automation tool designed to streamline document control workflows in Procore. This application allows users to select specific disciplines (e.g., Architectural, Electrical) from multiple projects, merge their drawings into a single PDF with a Table of Contents, and automatically upload the result to Procore while archiving previous versions.

## 🚀 Key Features

* **OAuth2 Authentication:** Secure login directly via Procore credentials.
* **Batch Processing:** Queue multiple projects and disciplines to run sequentially, respecting Procore API rate limits.
* **PDF Engine:** Merges hundreds of drawings, generates a clickable Table of Contents, and preserves existing Procore markups.
* **Smart Upload & Archiving:**
    * Detects existing files in the "Combined IFC" folder.
    * Automatically moves old files to an "Archive" subfolder with a unique timestamp (`_archived_1709...`).
    * Uploads the fresh PDF to the main folder.
* **Automation Scheduler:** Set recurring jobs (e.g., "Every Friday at 5:30 PM CST") that run automatically in the background.
* **Timezone Aware:** Schedules respect the user's local timezone (e.g., Mexico City, EST).
* **Real-Time Dashboard:** Visual progress bars, status updates, and direct download links for backup.

## 🛠️ Tech Stack

* **Backend:** Python 3.9+, FastAPI
* **Database:** SQLite (SQLAlchemy ORM)
* **PDF Processing:** PyMuPDF (fitz)
* **Scheduling:** APScheduler, Pytz
* **Frontend:** HTML5, JavaScript (Vanilla), Jinja2 Templates
* **Server:** Uvicorn (ASGI)

---

## ⚙️ Prerequisites

1.  **Python 3.10+** installed on your machine.
2.  A **Procore Developer Account**.
3.  A registered **Procore App** (to get Client ID and Secret).

## 📥 Installation

1.  **Clone the repository** (or unzip the project folder):
    ```bash
    cd procore-pdf-merger
    ```

2.  **Create a virtual environment (Recommended):**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows use: venv\Scripts\activate
    ```

3.  **Install dependencies:**
    ```bash
    pip install fastapi uvicorn requests python-dotenv sqlalchemy pymupdf jinja2 apscheduler pytz
    ```

## 🔑 Configuration

1.  Create a file named `.env` in the root directory.
2.  Add your Procore App credentials:

    ```ini
    PROCORE_CLIENT_ID=your_client_id_here
    PROCORE_CLIENT_SECRET=your_client_secret_here
    # Optional: Override base URL if using Sandbox
    # PROCORE_BASE_URL=[https://api.procore.com](https://api.procore.com)
    ```

3.  **Important:** In your Procore Developer Portal, ensure the **Redirect URI** for your app is set to:
    `http://localhost:8000/callback`

## 🏃‍♂️ How to Run

1.  **Start the Server:**
    ```bash
    uvicorn app:app --reload
    ```

2.  **Access the Dashboard:**
    Open your browser and go to: [http://localhost:8000](http://localhost:8000)

3.  **First Login:**
    You will be redirected to Procore to log in. This authorizes the app and saves the initial Refresh Token to the database for future scheduled jobs.

## 📖 Usage Guide

### 1. Manual Batch Run
1.  Select a **Project** from the dropdown.
2.  Select the **Disciplines** you need (e.g., Architectural, Structural).
3.  Click **"⬇️ Add to Queue"**.
4.  Repeat for as many projects as needed.
5.  Click **"Run Now"** to execute the queue immediately.

### 2. Scheduling Automation
1.  Build your Queue as described above.
2.  Click **"Schedule Recurring"**.
3.  Select the **Day** (e.g., Friday) and **Time** (e.g., 17:30).
4.  Click **"✅ Save Automation"**.
    * *Note: The system detects your local browser timezone and ensures the server runs the job at your correct local time.*

### 3. Folder Logic
The app expects the following folder structure in Procore Documents:
* `Drawing & Specs` (Root)
    * `Combined IFC` (Target Folder)
        * `Archive` (Created automatically if missing)

## ⚠️ Troubleshooting

* **"No Projects Found" / Empty List:**
    Your access token has expired. Go to `http://localhost:8000/login` to force a fresh login.
* **Database Errors:**
    If you modified the code or schema, delete the `jobs.db` file and restart the server. The app will regenerate a fresh database automatically.
* **Upload Failed (403 Forbidden):**
    Ensure the Procore user account you logged in with has "Standard" or "Admin" permissions on the Documents tool for that specific project.