Currently, the orders, order_items, returns, and, return_items data is coming via a JSON file which is dumped in ADLS.

Now, these information will come from the following two sources:

- A Postgres database hosted on `Supabase` (Free-Tier)
- Kafka hosted on `Confluent Cloud` (Free-Tier)

I have created a new project named `ECOMM` in Supabase.

I have created the Confluence account and created `ecomm_0` cluster.
