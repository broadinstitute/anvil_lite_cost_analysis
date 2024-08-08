# anvil_lite_cost_analysis

This is a simple utility package for the "AnVIL Lite" Cost Analysis notebook.

This repository is public so that it can be cloned into Jupyter VMs on Terra without authentication.
Don't commit any sensitive information here!

## Getting started

The latest version of the notebook is available at the top level of this repository.

**Important: before doing anything else, you should make a copy of the notebook and put it on the same directory level as this repository.** Some of the following steps may assume that directory structure.

There should be one or more cells near the top of the notebook that:
1. clones the repository, and 
2. installs the package using `!` notation, and 
3. imports the package under the alias `alca`.

```
!git clone https://github.com/broadinstitute/anvil_lite_cost_analysis.git
!pip install ./anvil_lite_cost_analysis

...

from anvil_lite_cost_analysis import alca
```

If you are updating this repo and want to use your branch from within a Jupyter Notebook, you'll need to checkout your branch from a terminal in Jupyter:
1. File -> New Launcher
2. Select Terminal
3. In the terminal:
 ```
 cd anvil_lite_cost_analysis
 git checkout <your-branch>
 ```