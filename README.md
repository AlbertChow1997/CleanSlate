https://clean-slate-mauve.vercel.app/

# CleanSlate

CleanSlate is a hackathon-ready Data Cleanup Assistant built around Daytona Sandbox.

It lets a user upload a messy CSV, runs a cleanup pipeline, and returns:

- a cleaned CSV
- a JSON summary
- an HTML report
- before/after metrics

## Project Structure

```text
backend/
  app/
    main.py
    config.py
    routes/
    services/
    templates/
    static/
frontend/
sample_data/
```

## Run Locally

1. Create a virtual environment and install dependencies.
2. Start the API:

```bash
uvicorn backend.app.main:app --reload
```

3. Open `http://127.0.0.1:8000`

## Environment Variables

- `DAYTONA_API_KEY`: Daytona API key
- `DAYTONA_TARGET`: Optional Daytona target/server
- `DAYTONA_API_URL`: Optional Daytona API URL
- `DAYTONA_SERVER_URL`: Optional Daytona server URL
- `USE_DAYTONA`: Set to `true` to execute jobs in Daytona instead of local fallback

If `USE_DAYTONA` is not enabled or the Daytona SDK is unavailable, the app uses the same cleanup pipeline locally so the demo still works.
