from fastapi.templating import Jinja2Templates

# Keep a single, minimal Jinja setup to avoid accidental environment corruption.
templates = Jinja2Templates(directory="app/templates")
