# Group Film Night Deploy Bundle

This bundle is ready to run on any host that can start a Python web process.

## Start command

```bash
GROUP_FILM_NIGHT_DIR=. python app.py --host 0.0.0.0 --port ${PORT:-8048}
```

## Included files

- `app.py`: standalone server
- `group-state.json`: initial shared state
- `dashboard/`: static dashboard assets
- `Procfile`: simple web entrypoint
- `Dockerfile`: portable deploy option for Railway, Render, Northflank, Fly, etc.

## Notes

- The app writes live changes back into `group-state.json`.
- For production, mount that file on persistent storage if your host supports it.
- `GROUP_FILM_NIGHT_DIR=.` tells the server to use this bundle folder for state and dashboard assets.
- On Northflank, keep `GROUP_FILM_NIGHT_DIR=/app` and add `GROUP_FILM_NIGHT_STATE_PATH=/data/group-state.json` if you attach a persistent volume at `/data`.
- The server reads `PORT` automatically.
- No external Python dependencies are required.
