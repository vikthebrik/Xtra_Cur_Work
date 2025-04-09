#Xtra_Cur_Work
This repository is a growing collection of extracurricular code, work-related scripts, and personal experiments developed outside of formal university coursework. While many of these projects may not run as-is outside of their original environments, they are documented here for reference, inspiration, and future iteration.

#Current Contents
##RACS/
Work done within the Research Advanced Computer Services Department at UO, which manages Talapas: our Heterogenous HPC cluster. Learn more here: [RACS](https://racs.uoregon.edu/services)

###automated_account_requests.py
Contains a python script designed to automate IMPORTANT: The script assumes a specific directory structure and module setup within the Talapas environment, and will not function properly outside of this context.

###Purpose:
Automates a portion of workflow regarding account request approval submitted by new users of the Talapas cluster. Usees the JIRA API to scrape data (mutliple account requests) directly from our portal and emails the corresponding PI from their PIRG. 

###Installation & Usage
No global installation is provided, as each script is intended for a specific environment or machine. Some projects are tightly coupled to local or institutional setups (e.g., Talapas cluster) and may not run without modification.

Feel free to browse, reuse logic, or adapt for your own workflow — but note that:

#🔧 Some code may only work in its intended environment.

##To Be Continued...
More tools and side projects will be added as they evolve — including cluster automation, small AI/ML experiments, and utility scripts for research or org work.

Stay tuned!