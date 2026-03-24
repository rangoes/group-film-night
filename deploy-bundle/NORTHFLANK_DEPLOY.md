# Northflank Deploy

This is the simplest path to get the app publicly reachable on Northflank.

## Cost

Northflank's pricing page currently lists a free `Sandbox` tier with always-on compute and free services. If you later outgrow that, you can move to a paid plan.

Pricing:
- https://northflank.com/pricing

## Recommended path

Northflank is easiest when it can build from a Git repository. The simplest setup is:

1. Create a new GitHub repository, for example `group-film-night`.
2. Upload the contents of this `deploy-bundle` folder to that repository.
3. In Northflank, create a new project.
4. Add a new service from that Git repository.
5. Let Northflank build with the included `Dockerfile`.
6. Set these environment variables:
   - `GROUP_FILM_NIGHT_DIR=/app`
7. Optional but recommended for persistence:
   - attach a persistent volume at `/data`
   - set `GROUP_FILM_NIGHT_STATE_PATH=/data/group-state.json`

## What happens without persistent storage

The app still works, but changes to the shared state can be lost whenever the service is rebuilt or restarted.

## Start behavior

The container already respects the `PORT` variable that Northflank provides.
You do not need to change the command if you deploy from the bundled `Dockerfile`.
