# Analysis of New York City's 311 data
#### An exploratory analysis of New York city's 311 data to understand patterns and draw inferences

**Group Details**

| Name            | NetID  |
|-----------------|--------|
| Shaival Dalal   | sd3462 |
| Rahul Keshwani  | ryk248 |

---
**Dependencies**:

| Name              | Version       | Use                                         |
|-------------------|---------------|---------------------------------------------|
| Python            | 3.4.4         | Coding solutions                            |
| Apache Spark      | 2.2.0         | Executing Python code on multicluster setup |
| Java              | 1.8.0_72      | Needed by Spark version 2.2.0               |
| Plotly            | 1.31.2        | Needed for data visualisation               |

---
How to run the script:<br/>
* To run the data cleaning script, pass the below listed command in your shell. Make sure spark2 is in your environment path <br/>
`spark2-submit Cleaning.py <datafilename.csv>`<br/>
* Similarly, run the data analysis script by using the below listed command <br/>
`spark2-submit Analysis.py <datafilename.csv>`

---
Project report: [Available here](https://drive.google.com/file/d/1YZQSQpayjXccRBOwKdsOJZXpUZUKuq7P/view?usp=sharing)
