# Some Dev Tutorials/info for Mark
Hey there Mark this is a markdown file (.md) this is how devs prefer to create readable documents.
For github and repos use this format not word docs or text files. They are prefered because its simple and can format most use cases well enough; and file ssize is small.

It's useful to keep a link to a cheatsheet for editing markdowns [Markdown Cheatsheet](https://www.markdownguide.org/cheat-sheet/).
Here's some things you can do in Mardown:

---

# Markdown Example Title Block

> This project analyzes spatial data and loads it into BigQuery for GIS operations. 

*italicized texts* 

**bold text**

## Header1

- bullets
- bullets
1. numbers
2. numbers

gifs:

![Funny cat](https://media.giphy.com/media/JIX9t2j0ZTN9S/giphy.gif)

code blocks: `import pandas as pd`

### Header2
Longer code blocks:
```python
import geopandas as gpd

gdf = gpd.read_parquet("data.parquet")
gdf["geometry"] = gdf.geometry.to_wkt()
gdf.to_parquet("converted.parquet", index=False)
```

---

## How to git for Mark
GitHub is where the repo for this work is stored @ [xentity_blm_dqimp_big_query](https://github.com/xentitycorporation/xentity_blm_dqimp_big_query)

### Steps to sync local folder with latest repo files and commit a change
1. Open Git Bash command prompt from Windows menu
2. `cd /path/to/local/repo`
3. `git pull origin main`
    - this will sync your local folder to the current repo
4. `code .`
    - this opens the repo in VS Code (or whatever default editor you have)
5. Open the README.md
6. Go to the *Work Log* section
7. Add a new subheader of today's date
8. Add a bullet
    * Mark learned to git
9. Now run `git add .`
    - this adds all of your changes to staging area
10. `git commit -m "updated README"
    - this logs what changes you made, standard is to keep brief
11. `git push -u origin main`
12. Congrats! You pushed a commit to the repo!

