# macOS Terminal Setup (from zero dependencies)

## 1) Install Homebrew (if missing)
```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
echo 'eval "$(/opt/homebrew/bin/brew shellenv)"' >> ~/.zprofile
eval "$(/opt/homebrew/bin/brew shellenv)"
```

## 2) Install Git + Python
```bash
brew install git
brew install python@3.12
python3 --version
git --version
```

## 3) Install Ollama
```bash
brew install ollama
ollama --version
```

## 4) Pull local models (pick for your machine)
```bash
ollama pull qwen3:8b
ollama pull qwen3:14b
# stronger machine optional:
ollama pull qwen3-coder:30b
```

## 5) Clone repo
```bash
git clone <YOUR_REPO_URL> Lead-enrichment-program
cd Lead-enrichment-program
```

## 6) Create virtual environment
```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
```

## 7) Install Python dependencies
```bash
pip install -r requirements.txt
```

## 8) Install Playwright browser
```bash
playwright install chromium
```

## 9) Create env file
```bash
cp .env.example .env
```

## 10) Choose model defaults in `.env`
Recommended:
- lower memory: `DEFAULT_SCHEMA_INFERENCE_MODEL=qwen3:14b`, `DEFAULT_ENRICHMENT_MODEL=qwen3:8b`, `DEFAULT_QUERY_GENERATION_MODEL=qwen3:8b`
- stronger machine: `DEFAULT_SCHEMA_INFERENCE_MODEL=qwen3-coder:30b`, `DEFAULT_ENRICHMENT_MODEL=qwen3:14b`, `DEFAULT_QUERY_GENERATION_MODEL=qwen3:8b`

## 11) Start Ollama and app
Open terminal A:
```bash
ollama serve
```
Open terminal B:
```bash
cd Lead-enrichment-program
source .venv/bin/activate
uvicorn app.main:app --reload
```

## 12) Run app
Open http://127.0.0.1:8000 and upload CSV.

## Troubleshooting
- `Connection refused localhost:11434` → run `ollama serve`.
- `Selected model ... is not installed` → `ollama pull <model>`.
- Playwright errors → run `playwright install chromium` again.
- Slow/failed 30B model → switch schema model to 14B/8B from run preview form or `.env`.
