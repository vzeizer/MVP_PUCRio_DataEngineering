## MVP Data Engineering - Data Science & Analytics Post-Graduation Program of PUC-Rio

### Student: Dr. Vagner Zeizer Carvalho Paes
### Professors:  Tatiana Escovedo, Fernanda Baião, Marcos Villas, Anthony Seabra, Silvio Alonso, and Victor Almeida

## Files and Folders Structures

- ./
    - DataCatalog_MVPIII.xlsx: it contains the Data Catalog of the *Crimes in Paraná* dataset in the *bronze* layer;
    - MVP_III_CrimesPR.ipynb: it contains the ipython notebook summarizing MVP's results;
    - MVP_III_CrimesPR.pdf: the pdf file of the ipython notebook, which summarizes MVP's results.

- ./assets/
    - It contains many figures regarding the screenshots used to evidence the work has been done, as shown and fully documented in the ipython notebook *MVP_III_CrimesPR.ipynb*.

- ./data/
    - cleaned_CrimesPR.csv: it contains "cleaned data" (same as in the *silver* layer) regarding different types of Crimes in the State of Paraná, Brazil, over the 2018-2023 years, which was obtained after cleaning/transforming the data to an appropriate format for subsequent data analysis;
    - CrimesPR.csv: it contains raw data (same as in the *bronze* layer) regarding different types of Crimes in the State of Paraná, Brazil, over the 2018-2023 years;
    - CrimesPR_Metropolitan_Curitiba_statistics.xlsx: it contains descriptive statistics concerning the main municipalities in the metropolitan area of Curitiba.

- ./notebooks/
    - DataBricks_ETL_SQLqueries_PySpark.ipynb: it shows the script used in DataBricks in order to run the full ETL pipeline and perform SQL queries;
    - EDA_CrimesPR.ipynb: it shows a comprehensive Exploratory Data Analysis (*EDA*) procedure in Python;
    - ETL.ipynb: it shows the full Extract Transform and Load (*ETL*) pipeline in Python code;
    - queries_crimes.sql: it shows the queries written in SQL used to answer the business questions defined in the beginning of the project.

## LICENSE

The license for this public, that data was made available by the Brazilian government, is the **[Creative Commons Attribution 4.0 International (CC BY 4.0) license](https://creativecommons.org/licenses/by/4.0/deed.en)**. This license allows anyone to reuse, distribute, and modify the data for any purpose, including commercial purposes, as long as they give appropriate credit to the original source.
