# InBank Internship Task Solution
This document presents solutions to the InBank internship task. The task contains two parts which are explained below with the solution.

## Part-1: Aggregated Monthly Payments (SQL)
### Problem
**Using the following four tables (payments, currencies, exchange rates, blacklist), please write a query to return the amounts in euros aggregated by
transaction_date.**

Be informed that:

* Resulting table should provide sum in EUR, use exchange rate table if payments are in other currencies
* Due to a data quality error payments table got some payments in discontinued currencies (where currencies.end_date is not NULL), they should be excluded from the final result
* Do pay attention to the blacklist table - as payments from blacklisted users should also be excluded

**If needed - SQL script to create and populate tables could be found in [GitHub](https://github.com/oleksandr-cherednychenko-inbank/data_engineering_test_2023/blob/main/aggregated_monthly_payments.sql)**



### Solution

```sql
SELECT 
  transaction_date, 
  round(sum(amount_eur),2) 
FROM
  (
    SELECT
      transaction_date, 
      if(currency=
        ( 
        SELECT                
          currency_id 
        FROM 
          currencies 
        WHERE 
          currency_code='EUR'
        ),amount,amount*exchange_rate_to_eur) as amount_eur 
    FROM 
      payments p 
    LEFT JOIN 
      currency_rates c on p.currency=c.currency_id and p.transaction_date = c.exchange_date 
    WHERE 
    user_id_sender not in (select user_id from blacklist) 
    AND 
    currency not in (select currency_id from currencies where end_date is not null)
    ) as c 
GROUP BY 
  transaction_date;

```

## Part-2:  Weekend Data Processing (Python)
**Disclaimer:** All names, characters, and incidents portrayed in this assignment are fictitious, no identification with actual products, places, companies or individuals is intended. **

### Problem
In order to fulfill regulatory requirements, I-bank is obliged to provide
aggregated daily report to B-Authority. It’s been agreed that during workdays,
reports should be provided daily by 5 pm at latest. However, the agreement for
weekends is different - due to regular scheduled maintenance of B-Authority
servers, data for Saturday and Sunday should be aggregated and sent out in one
file on Monday mornings.

Task: please write a Python script that would combine data from 2 files (archive with files available in [GitHub](https://github.com/oleksandr-cherednychenko-inbank/data_engineering_test_2023/blob/main/weekend_data_processing.zip)) into one. Overall file structure could be preserved -but add information about combined file generation date (in additional column).

### Solution
The problem is to aggregate data for Saturday and Sunday. The solution skipped `metric_date` due to the candidate's unclarity over which date should go in the `metric_date` column in the final file.


```python
import pandas as pd
import datetime

# loading csv files
f1 = pd.read_csv('./weekend_data_processing/data_2023-02-11.csv',sep=';')
f2 = pd.read_csv('./weekend_data_processing/data_2023-02-12.csv',sep=';')

# concatenating files
f3 = pd.concat([f1,f2])


# aggregating data
df = f3.groupby(['company','metric_id','metric_desc']).sum('metric').reset_index()

# adding extra column with file generation date
combined_date = datetime.datetime.now().date()
df['combined_date'] = combined_date

# saving file
filename = f'./weekend_data_processing/data_{combined_date}.csv'
df.to_csv(filename, index=False)
```


```python
df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>company</th>
      <th>metric_id</th>
      <th>metric_desc</th>
      <th>metric_value</th>
      <th>combined_date</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>i-bank</td>
      <td>1</td>
      <td>active contracts</td>
      <td>22000</td>
      <td>2024-04-10</td>
    </tr>
    <tr>
      <th>1</th>
      <td>i-bank</td>
      <td>2</td>
      <td>active customers</td>
      <td>100500</td>
      <td>2024-04-10</td>
    </tr>
    <tr>
      <th>2</th>
      <td>i-bank</td>
      <td>3</td>
      <td>employees count</td>
      <td>201</td>
      <td>2024-04-10</td>
    </tr>
    <tr>
      <th>3</th>
      <td>i-bank</td>
      <td>4</td>
      <td>active partners</td>
      <td>2005</td>
      <td>2024-04-10</td>
    </tr>
    <tr>
      <th>4</th>
      <td>i-bank</td>
      <td>5</td>
      <td>unique products</td>
      <td>10</td>
      <td>2024-04-10</td>
    </tr>
  </tbody>
</table>
</div>




```python

```
