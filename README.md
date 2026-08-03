# YAMI HOSTING Panel

A modern, mobile-first hosting control panel for deploying and managing Python applications, Discord bots, and Telegram bots on any hosting platform.

## Features

- **Server Management** — Create, start, stop, restart Python servers
- **Real-time Console** — Terminal with command execution and live log streaming
- **File Manager** — Upload, edit, create, delete, download files and folders
- **Backup System** — Create, restore, and download server snapshots
- **Startup Configuration** — Set startup files, requirements, Git auto-deploy
- **Resource Monitoring** — Live CPU, RAM, Disk, and Uptime stats
- **User System** — Sign up, login, admin panel, server limits
- **Mobile-First Design** — Optimized for all screen sizes
- **Webhook Notifications** — Discord webhook alerts for crashes and restarts

## Quick Deploy

### Render
[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com)

1. Create a new Web Service
2. Set Build Command: `pip install -r requirements.txt`
3. Set Start Command: `gunicorn --worker-class eventlet -w 1 --bind 0.0.0.0:$PORT main:app`
4. Add environment variable: `SESSION_SECRET` (generate a random 64-char hex string)
5. Add a Disk (1GB+) mounted at `/data`

### Railway
1. Deploy from GitHub repo
2. Set environment variables in Railway dashboard
3. Attach a volume mounted at `/data`

### fly.io / Heroku / Any Platform
```bash
pip install -r requirements.txt
gunicorn --worker-class eventlet -w 1 --bind 0.0.0.0:$PORT main:app
```

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `SESSION_SECRET` | Yes | Random 64-char hex string for session encryption |
| `DATA_DIR` | Yes | Persistent storage path (default: `/data`) |
| `CORS_ORIGINS` | No | Comma-separated CORS origins |

## Local Development
```bash
pip install -r requirements.txt
python main.py
# Open http://localhost:5000
```

## Requirements
- Python 3.9+
- Flask, Flask-SocketIO, Eventlet
- psutil for system monitoring
- SQLite for data storage

## Admin Login
Default admin credentials:
- Username: `CLAUD`
- Password: `09667664037`

Change immediately after first login from the admin panel.
