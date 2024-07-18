# ⏱️ Stay informed

*Get a global view of news in less than 5 minutes a day*

📚 Aggregate and clean news from a plurality of sources

📝 Create news summaries:

* KPI (ex: best topics of the days, popular keywords, trending by country...)
* Daily summary with LLM < 300 words


## Best practices for PR:

* Create a new branch from develop with the right [branch naming](https://medium.com/@abhay.pixolo/naming-conventions-for-git-branches-a-cheatsheet-8549feca2534) (ex : feature/login_system , bugfix/remove_env_file)
* Create a PR for merging on develop
    * Need to be validated by a peer
* Create a PR for merging develop on main
    * Need to be validated by a peer
* Merge develop on main

## Testing comment


## GCP connection

In order to works with GCP, you need to add you creds.json from a service account into the folder `credentials/` and you need to update the env `GCP_CREDS` too.