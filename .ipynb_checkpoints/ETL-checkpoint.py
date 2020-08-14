import numpy as np, pandas as pd

# Read in csv files from data directory
cons_info = pd.read_csv("./data/cons.csv")
cons_email = pd.read_csv("./data/cons_email.csv")
cons_subscription = pd.read_csv("./data/cons_email_chapter_subscription.csv")

# To make the "people" file, I only need primary email addresses
people = cons_email.drop(cons_email[cons_email['is_primary'] != 1].index)

# Dropping all the columns not needed to create the "people" file
people.drop(columns=['cons_email_type_id', 'is_primary', 'canonical_local_part', 'domain', 'double_validation', 'create_dt', 'create_app', 'create_user', 'modified_dt', 'modified_app', 'modified_user', 'status', 'note'], inplace=True)

# Getting the columns I need out of the cons_info dataframe
cons_info_clipped = cons_info[['cons_id', 'source', 'create_dt', 'modified_dt']]

# merging those onto my people dataframe
people = people.merge(cons_info_clipped, on='cons_id')

# Getting the last column I need - the isunsub from cons_subscription
# per the instructions, chapter_ids besides 1 are excluded - accomplishing this by setting isunsub to 0 where chapter_id != 1
cons_subscription.loc[cons_subscription.chapter_id != 1, 'isunsub'] = 0

# Left merging the unsub status onto the people dataframe
people = people.merge(cons_subscription[['cons_email_id', 'isunsub']], how='left', on='cons_email_id')

# as per instructions, emails not in the cons_subscription dataframe are assumed to be subscribed
people.loc[people.isunsub.isnull(), 'isunsub'] = 0

# We have all our columns now but need to clean it up to get to the schema the instructions require. I'm honestly uncertain what is meant
# by the 'code' column (description: "Source code") but the closest thing I could find was the 'source' column, so I'm including that.
# cons_email_id and cons_id were used for joins but aren't included in the schema, so I'm dropping them
people.drop(columns=['cons_email_id', 'cons_id'], inplace=True)

# Renaming columns to those specified in schema
people.rename(columns={'source': 'code', 'create_dt': 'created_dt', 'modified_dt': 'updated_dt', 'isunsub': 'is_unsub'}, inplace=True)

# Now to get the specified order
people = people[['email', 'code', 'is_unsub', 'created_dt', 'updated_dt']]

# is_unsub column should be a boolean
people['is_unsub'] = people['is_unsub'].astype('bool')

# last but not least, I want to clean up the 'code' column a little even though I'm uncertain what the end goal is. 
# There aren't too many unique sources, so they could be replaced with some sort of numeric code, but I'm just going to leave them in place. 
# The NaNs, though, I'm going to replace with the string 'none' to hopefully avoid potential issues
people.loc[people.code.isnull(), 'code'] = 'none'

# And now to output as a csv
people.to_csv("./data/people.csv", header=True, index=False)

# For the second part of the ETL exercise, creating the "acquisitions" file, I'm going to make a copy of part of this df to work with
acquisitions = people.drop(columns=['code', 'is_unsub'])

# We don't need the times for this, just the actual dates. I'm just going to slice the last nine characters off the created_dt column
acquisitions['created_dt'] = [entry[:-9] for entry in acquisitions['created_dt']]

# Using a groupby to create the actual "acquisition_facts" data, counting the number of emails added on each unique day
acquisition_facts = acquisitions[['email', 'created_dt']].groupby(by=['created_dt'], as_index=False).count()

# Renaming columns to those specified by the schema
acquisition_facts.rename(columns={'created_dt': 'acquisition_date', 'email': 'acquisitions'}, inplace=True)

# And now to write to csv
acquisition_facts.to_csv('./data/acquisition_facts.csv', index=False)