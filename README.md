# 🎯 About

![image](https://github.com/clv-dev/jira-clv-transformation/assets/113873442/1b6f80d9-60af-44ba-a6ba-6ba025a887b0)

- The goal is to implement an approach to get data from Jira platform for visualization with minnimal cost.
- An ELT pipelinne has been built to process data incrementally on update_date column.
- Scheduler is set up independently for Airbyte and DBT on weekly basis.
- Looker Studio uses transformed data to monitor member effort on Jira platform.

# 🛠️ Technology and Architecture
- [Jira Cloud for Sheets](https://workspace.google.com/marketplace/app/jira_cloud_for_sheets/1065669263016) is an add-on in Google Sheet to load information of a specific project in [Jira Software](https://www.atlassian.com/software/jira/guides/getting-started/introduction#what-is-jira-software).
- [Airbyte](https://github.com/airbytehq/airbyte) is an open-source application used to extract & load data. However, Google's products (Bigquery, Google Sheet) setting is not ultilized for making incremental mode in Airbyte since all columns are indicated as type string. Therefore, data is loaded into [Postgres](https://www.elephantsql.com/docs/index.html) and changed to type timestamp in prior.
- [Bigquery](https://cloud.google.com/bigquery) serves as the Data Warehouse for the whole project, providing flexibility and cost-effectiveness for data storage.
- [DBT](https://github.com/dbt-labs/dbt-core) is an open-source transformation tool, which mainly facilitates the implementaion of modern ELT pipeline. Besides, DBT also offers data quality checks using [DBT Tests](https://docs.getdbt.com/docs/build/data-tests)
