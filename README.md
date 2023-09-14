# FoxFeed

Furry-focussed custom feeds for [BlueSky](https://bsky.app/).

View the list of available feeds [here](https://bsky.probablyaweb.site/).

## Developing

Install:
- python3.8 or above
- some version of node (I'm running 18.17.0, required for tooling, not used at runtime)
- postgres

Set up an empty local DB

`cp .env.example .env` and fill out the settings

Then install the requirements:

```
pip install -r requirements.txt
prisma generate
```

Run the local server:

```
python -m server
```
