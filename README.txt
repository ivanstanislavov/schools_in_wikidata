The program has the following flow of execution:
Data Ingestion -> Data Cleaning -> Data Transformation -> Data Reconciliation -> Report -> WikiData Update

1. Data Ingestion
1.1 Data Ingestion from MES
Firstable the script acquires the data from the MES website. This logic is handled by the get_data_from_MES() function.
Since there are multiple datasets (in json format) organized in a snowflake schema, that comprise the entire school
dataset, several HTTP requests are necessary. Therefore, for each request an endpoint and output file name is defined.
Then the send_post_request(...) method is called in a for loop with the necessary parameters for each request. The
result is that all of the school data (distributed onto multiple files) is saved in the main directory where the
script is executed. The "requests" Python external library is used for creating the HTTP requests.

1.2 Data Ingestion from WikiData
The process_data_from_WikiData(...) method defines an instance of the "SPARQLWrapper" class from the "SPARQLWrapper" Python
library in order to send a SPARQL query to the WikiData Query Service. Then the query itself is defined and passed as
parameter to the setQuery(...) method of the object. After the results have been fetched, the data is extracted from the
request object and some data cleaning tasks are performed - column renaming, data type conversion, etc.

2. Data Cleaning
2.1 Data Cleaning for MES datasets
The process_data_from_MES(...) method handles the cleaning of data for the MES dataset. It drops unnecessary and duplicate 
columns. The function also renames some columns for better readability of the dataset. Data conversion of string to
numeric is performed on the 'id' column.

2.2 Data Cleaning for WikiData
The process_data_from_WikiData(...) handles the cleaning of data, it mainly consists of renaming columns and data type conversion.

3. Data Transformation
3.1 Data Transformation for MES datasets
The process_data_from_MES(...) method handles the transformation of data for the MES dataset. The function calls the 
clean_df_MES(...) helper function which handles the extraction of data from the nested structure of the raw MES 
dataset. Then the merge method of the Pandas library is used to merge each separate Pandas dataframe (corresponding 
to a json file from the MES site such as town, region, etc) into the public register dataframe (since the data is
originally into a snowflake schema).

3.2 Data Transformation for WikiData datasets
The process_data_from_WikiData() handles the data transformation such as extracting data from the nested result
object after the response from the WikiData Query Service has been received.

4. Data Reconciliation
The reconcilation of the datasets is done by calling the merge(...) method of the Pandas library. The merging
is done on the id (school code) which uniquely identifies each school and is the common column of both datasets.
The previous steps in data cleaning have ensured that the desired column of both datasets is renamed to the same
label.

5. Data Report
There are two different types of reports that the script can do - pdf file and text file - generate_report_pdf(...) and
generate_report_text_file(...) call. Both functions use the get_statistics(...) function call which returns the total
number of discrepancies in school location, count of spelling mistakes, count of stylistical mistakes, count of factological
mistakes. That function iterates each row of the reconcilidated dataset and marks it as spelling error, stylistical error or
factological error (based on whether the name is mispelled or the name consists of additional information or the name is simply
incorrect). In order to perform the error checking, the find_discrepancy(...) method is called on each row of the dataframe.
The generate_report_pdf(...) function uses the "FPDF" library to create a pdf file.
Both the text file and the pdf file consist of the following structure:
- Total number of discrepancies in school location:
- Count of spelling errors in locations:
- Count of stylistical errors in locations:
- Count of factual errors in locations:

List of all schools with spelling errors in their location:
List of all schools wit stylistical errors in their location:
List of all schools with factual errors in their location:

6. WikiData Update
The update of the data with the correct location of the schools (derived from the MES website) is done manually due to the
small amount of discrepancies found in both datasets.