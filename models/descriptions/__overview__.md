{% docs __overview__ %}

# Welcome to the Flipside Crypto Maya Models Documentation

## **What does this documentation cover?**

The documentation included here details the design of the Maya
tables and views available via [Flipside Crypto.](https://flipsidecrypto.xyz/insights/dashboards/maya) For more information on how these models are built, please see [the github repository.](https://github.com/FlipsideCrypto/maya-models)

## **How do I use these docs?**

The easiest way to navigate this documentation is to use the Quick Links below. These links will take you to the documentation for each table, which contains a description, a list of the columns, and other helpful information.

If you are experienced with dbt docs, feel free to use the sidebar to navigate the documentation, as well as explore the relationships between tables and the logic building them.

There is more information on how to use dbt docs in the last section of this document.

## **Quick Links to Table Documentation**

**Click on the links below to jump to the documentation for each schema.**

### Core Schema (`maya`.`CORE`.`<table_name>`)

- [core.dim_block](#!/model/model.maya_models.core__dim_block)
- [core.dim_midgard](#!/model/model.maya_models.core__dim_midgard)
- [core.fact_mayaname_change_events](#!/model/model.maya_models.core__fact_mayaname_change_events)
- [core.fact_set_mimir_events](#!/model/model.maya_models.core__fact_set_mimir_events)
- [core.fact_transfer_events](#!/model/model.maya_models.core__fact_transfer_events)
- [core.fact_transfers](#!/model/model.maya_models.core__fact_transfers)

### Defi Schema

- [defi.fact_active_vault_events](#!/model/model.maya_models.defi__fact_active_vault_events)
- [defi.fact_add_events](#!/model/model.maya_models.defi__fact_add_events)
- [defi.fact_block_pool_depths](#!/model/model.maya_models.defi__fact_block_pool_depths)
- [defi.fact_block_rewards](#!/model/model.maya_models.defi__fact_block_rewards)
- [defi.fact_bond_actions](#!/model/model.maya_models.defi__fact_bond_actions)
- [defi.fact_bond_events](#!/model/model.maya_models.defi__fact_bond_events)
- [defi.fact_daily_earnings](#!/model/model.maya_models.defi__fact_daily_earnings)
- [defi.fact_daily_pool_stats](#!/model/model.maya_models.defi__fact_daily_pool_stats)
- [defi.fact_daily_tvl](#!/model/model.maya_models.defi__fact_daily_tvl)
- [defi.fact_failed_deposit_messages](#!/model/model.maya_models.defi__fact_failed_deposit_messages)
- [defi.fact_fee_events](#!/model/model.maya_models.defi__fact_fee_events)
- [defi.fact_gas_events](#!/model/model.maya_models.defi__fact_gas_events)
- [defi.fact_inactive_vault_events](#!/model/model.maya_models.defi__fact_inactive_vault_events)
- [defi.fact_liquidity_actions](#!/model/model.maya_models.defi__fact_liquidity_actions)
- [defi.fact_outbound_events](#!/model/model.maya_models.defi__fact_outbound_events)
- [defi.fact_pending_liquidity_events](#!/model/model.maya_models.defi__fact_pending_liquidity_events)
- [defi.fact_pool_balance_change_events](#!/model/model.maya_models.defi__fact_pool_balance_change_events)
- [defi.fact_pool_block_balances](#!/model/model.maya_models.defi__fact_pool_block_balances)
- [defi.fact_pool_block_fees](#!/model/model.maya_models.defi__fact_pool_block_fees)
- [defi.fact_pool_block_statistics](#!/model/model.maya_models.defi__fact_pool_block_statistics)
- [defi.fact_pool_events](#!/model/model.maya_models.defi__fact_pool_events)
- [defi.fact_refund_events](#!/model/model.maya_models.defi__fact_refund_events)
- [defi.fact_reserve_events](#!/model/model.maya_models.defi__fact_reserve_events)
- [defi.fact_rewards_event_entries](#!/model/model.maya_models.defi__fact_rewards_event_entries)
- [defi.fact_rewards_events](#!/model/model.maya_models.defi__fact_rewards_events)
- [defi.fact_send_message_events](#!/model/model.maya_models.defi__fact_send_message_events)
- [defi.fact_slash_liquidity_events](#!/model/model.maya_models.defi__fact_slash_liquidity_events)
- [defi.fact_stake_events](#!/model/model.maya_models.defi__fact_stake_events)
- [defi.fact_streamling_swap_details_events](#!/model/model.maya_models.defi__fact_streamling_swap_details_events)
- [defi.fact_swaps](#!/model/model.maya_models.defi__fact_swaps)
- [defi.fact_swaps_events](#!/model/model.maya_models.defi__fact_swaps_events)
- [defi.fact_total_block_rewards](#!/model/model.maya_models.defi__fact_total_block_rewards)
- [defi.fact_total_value_locked](#!/model/model.maya_models.defi__fact_total_value_locked)
- [defi.fact_update_node_account_status_events](#!/model/model.maya_models.defi__fact_update_node_account_status_events)
- [defi.fact_withdraw_events](#!/model/model.maya_models.defi__fact_withdraw_events)

### Governance Schema

- [gov.fact_new_node_events](#!/model/model.maya_models.gov__fact_new_node_events)
- [gov.fact_set_ip_address_events](#!/model/model.maya_models.gov__fact_set_ip_address_events)
- [gov.fact_set_node_keys_events](#!/model/model.maya_models.gov__fact_set_node_keys_events)
- [gov.fact_set_version_events](#!/model/model.maya_models.gov__fact_set_version_events)
- [gov.fact_slash_amounts](#!/model/model.maya_models.gov__fact_slash_amounts)
- [gov.fact_slash_points](#!/model/model.maya_models.gov__fact_slash_points)
- [gov.fact_validator_request_leave_events](#!/model/model.maya_models.gov__fact_validator_request_leave_events)

### Price Schema

 - [price.factcacao_price](#!/model/model.maya_models.price__fact_cacao_price)
 

## **Data Model Overview**

While maya models are built a few different ways, they are organized into three layers of sql models: **bronze, silver, and gold (or core).**

- Bronze: Data is loaded in from the source as a view
- Silver: All necessary parsing, filtering, de-duping, and other transformations are done here
- Gold (or core): Final views and tables that are available publicly

The dimension tables are sourced from a variety of on-chain and off-chain sources.

Convenience views (denoted ez\_) are a combination of different fact and dimension tables. These views are built to make it easier to query the data.

## **Using dbt docs**

### Navigation

You can use the `Project` and `Database` navigation tabs on the left side of the window to explore the models in the project.

### Database Tab

This view shows relations (tables and views) grouped into database schemas. Note that ephemeral models are _not_ shown in this interface, as they do not exist in the database.

### Graph Exploration

You can click the blue icon on the bottom-right corner of the page to view the lineage graph of your models.

On model pages, you'll see the immediate parents and children of the model you're exploring. By clicking the Expand button at the top-right of this lineage pane, you'll be able to see all of the models that are used to build, or are built from, the model you're exploring.

Once expanded, you'll be able to use the `--models` and `--exclude` model selection syntax to filter the models in the graph. For more information on model selection, check out the [dbt docs](https://docs.getdbt.com/docs/model-selection-syntax).

Note that you can also right-click on models to interactively filter and explore the graph.

### **More information**

- [maya on Flipside Crypto](https://flipsidecrypto.xyz/insights/dashboards/maya)
- [Tutorials](https://docs.flipsidecrypto.com/our-data/tutorials)
- [Github](https://github.com/FlipsideCrypto/maya-models)
- [What is dbt?](https://docs.getdbt.com/docs/introduction)

{% enddocs %}