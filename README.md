# Analytics platform for Omnichannel Retail
The problem:
The retail company sales on the following channels:
- Physical Store
- E-comerce

They have some issues to solve as of now:
 - They don't know which products have better rotation
 - They don't detect stock breaks
 - They are not clear about the  customer behaviour

# This project is the data platform to solve those issues
Out objective is to work on their data, this project will:
- Centralize
- Process
- Analize
- Visualize

# Modeling Decisions

*Country and Customer*
<br/>The Country attribute was modeled within the Customer dimension because it describes the customer entity rather than the sales transaction. Creating a separate Country dimension would introduce unnecessary snowflaking without analytical benefit.
<br/>
<br/>*Fact Table Granularity*
<BR/>The fact table is modeled at the invoice line level, where each record represents a product sold within a specific invoice.
<br/>


# Incremental Load Strategy
<br/>*Late arriving data handling*
<br/> The source system allows historical updates within the current and previous month, so the pipeline implements a sliding window incremental strategy that reloads the last two months of data. This approach ensures late updates are captured while avoiding full historical reloads.
<br/> In mature pipelines this window can be combined with a watermark or control table to track the last processed timestamp.
