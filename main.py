from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import requests
import json
from fpdf import FPDF

def send_post_request(url, payload, file_to_save):
    # Send the POST request
    response = requests.post(url, json=payload)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON content
        data = response.json()

        # Write the JSON data to a file
        with open(file_to_save, 'w') as f:
            json.dump(data, f, indent=4)

        print(f"JSON data saved to {file_to_save}")
    else:
        print(f"Failed to retrieve data. HTTP Status code: {response.status_code}")


def get_data_from_MES():
    # Define MES URL with school data
    url = "https://ri-api.mon.bg/data/get/public-transformTypes"

    # Define the payload for the requests
    payload = {"instType":[1],"isRIActive":1}
    mes_data_reference = [
        {
            "url": "https://ri-api.mon.bg/data/get/public-transformTypes",
            "file_to_save": "public-transformTypes.json"
        },
        {
            "url": "https://ri-api.mon.bg/data/get/financialSchoolTypesNoLimit",
            "file_to_save": "financialSchoolTypesNoLimit.json"
        },
        {
            "url": "https://ri-api.mon.bg/data/get/detailedSchoolTypes",
            "file_to_save": "detailedSchoolTypes.json"
        },
        {
            "url": "https://ri-api.mon.bg/data/get/town",
            "file_to_save": "town.json"
        },
        {
            "url": "https://ri-api.mon.bg/data/get/region",
            "file_to_save": "region.json"
        },
        {
            "url": "https://ri-api.mon.bg/data/get/municipality",
            "file_to_save": "municipality.json"
        },
        {
            "url": "https://ri-api.mon.bg/data/get/public-register",
            "file_to_save": "public-register.json"
        }
    ]

    # Send the requests to MES api endpoint
    for item in mes_data_reference:
        send_post_request(item["url"], payload, file_to_save=item["file_to_save"])

def clean_df_MES(df, label_column):
    df['code'] = df['data'].apply(lambda x: x['code'])
    df[label_column] = df['data'].apply(lambda x: x['label'])
    df.drop(columns=['data', 'status'], inplace = True)

    return df

def merge_into_register(df_public_register, df, df_col, left, right):
    df = clean_df_MES(df, df_col)
    df_public_register = df_public_register.merge(df, left_on=left, right_on=right, how='inner')
    df_public_register.drop(columns=[left, right], inplace = True)

    return df_public_register

def process_data_from_MES():
    # Retrieve school data from MES
    get_data_from_MES()

    # Read the data into dataframes
    df_town = pd.read_json('town.json')
    df_region = pd.read_json('region.json')
    df_public_transform = pd.read_json('public-transformTypes.json')
    df_public_register = pd.read_json('public-register.json')
    df_municipality = pd.read_json('municipality.json')
    df_financial_school = pd.read_json('financialSchoolTypesNoLimit.json')
    df_detailed_school = pd.read_json('detailedSchoolTypes.json')
    df_public_register = pd.DataFrame(df_public_register['data'].values[0])
    
    # Merge town data about schools into public register
    df_public_register = merge_into_register(df_public_register, df_town, 'town_name', 'town', 'code')

    # Clean region data about schools into public register
    df_public_register = merge_into_register(df_public_register, df_region, 'region_name', 'region', 'code')

    # Merge municipality data about schools into public register
    df_public_register = merge_into_register(df_public_register, df_municipality, 'municipality_name', 'municipality', 'code')

    # Merge financial data about schools into public register
    df_public_register = merge_into_register(df_public_register, df_financial_school, 'financialSchoolType_name', 'financialSchoolType', 'code')

    # Merge transform data about schools into public register
    df_public_register = merge_into_register(df_public_register, df_public_transform, 'transformType_name', 'transformType', 'code')

    # Merge detailed data about schools into public register
    df_public_register = merge_into_register(df_public_register, df_detailed_school, 'detailedSchoolType_name', 'detailedSchoolType', 'code')

    df_public_register['id'] = df_public_register['id'].astype(int)
    df_public_register.rename(columns={'name': 'name_mes', 'town_name': 'location_mes'}, inplace=True)

    return df_public_register[['id', 'name_mes', 'location_mes']].drop_duplicates('id').reset_index()


def process_data_from_WikiData():
    # Define the query
    sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
    sparql.setQuery("""
        SELECT DISTINCT ?school ?schoolLabel ?city ?cityLabel ?code ?codeLabel WHERE {
        ?school wdt:P31 wd:Q3914;   # School instance
                wdt:P17 wd:Q219;    # Located in Bulgaria
                wdt:P9034 ?code.    # School Code (NEISPOU)

        OPTIONAL {
            ?school wdt:P131 ?city.     # Located in the administrative territorial entity
            ?city wdt:P31 ?cityType.
            FILTER(?cityType != wd:Q7553685)  # Exclude instances of Q7553685
        }

        SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],bg,en". }
        }
    """)
    sparql.setReturnFormat(JSON)

    # Retrieve results
    results = sparql.query().convert()
    results_df = pd.io.json.json_normalize(results['results']['bindings'])
    results_df.rename(columns={"schoolLabel.value":"name_wikidata", "cityLabel.value": "location_wikidata", "codeLabel.value": "id"}, inplace=True)
    results_df['id'] = results_df['id'].astype(int)

    return results_df[["name_wikidata", "location_wikidata", "id", "school.value"]].drop_duplicates('id').reset_index()


def find_discrepancy(row):
    row['spelling_error'] = False
    row['stylistic_error'] = False
    row['factual_error'] = False

    # Transform location data and compare by letters
    if row['location_mes'].upper() == row['location_wikidata'].upper():
        row['spelling_error'] = True
    # Transform location data and compare by inclusion
    elif row['location_mes'].upper() in row['location_wikidata'].upper():
        row['stylistic_error'] = True
    # Transform location data and compare for different names
    elif row['location_mes'].upper() != row['location_wikidata'].upper():
        row['factual_error'] = True

    return row
     

def get_statistics(df_discrepancies):
    # Transform the location names to string data types
    df_discrepancies['location_mes'] = df_discrepancies['location_mes'].astype(str)
    df_discrepancies['location_wikidata'] = df_discrepancies['location_wikidata'].astype(str)
    
    # Apply function per row to determine the type of error
    df_discrepancies = df_discrepancies.apply(find_discrepancy, axis=1)

    # Get count for each error type
    total_number_of_location_discrepancies = len(df_discrepancies)
    number_of_location_spelling_errors = df_discrepancies['spelling_error'].sum()
    number_of_location_stylistic_errors = df_discrepancies['stylistic_error'].sum()
    number_of_incorrect_locations = df_discrepancies['factual_error'].sum()

    return df_discrepancies, total_number_of_location_discrepancies, number_of_location_spelling_errors, number_of_location_stylistic_errors, number_of_incorrect_locations


def write_section_to_pdf(pdf, title, df):
    # Create a section title
    pdf.set_font('DejaVu', '', 12)
    pdf.cell(200, 10, txt=title, ln=True)
    pdf.set_font('DejaVu', '', 7)

    # Add rows for schools
    custom_index = 1
    for index, school in df.iterrows():
        pdf.multi_cell(200, 10, txt="{}. Име: {} ; НЕИСПУО: {} ; МОН: {} ; WikiData: {}".format(custom_index, school['name_mes'], school['id'], school['location_mes'], school['location_wikidata']))
        custom_index+=1
    
    pdf.ln(10)


def write_section_to_text_file(file, title, df):
    # Create a section title
    file.write(f"{title}\n\n")

    # Add rows for schools
    custom_index = 1
    for index, school in df.iterrows():
        file.write(f"{custom_index}. Име: {school['name_mes']} ; НЕИСПУО: {school['id']} ; МОН: {school['location_mes']} ; WikiData: {school['location_wikidata']}\n")
        custom_index += 1

    file.write("\n")


def generate_report_text_file(df):
    # Get statistical data
    df_discrepancies = df.loc[df['location_wikidata'] != df['location_mes']]
    df_discrepancies, total_number_of_location_discrepancies, number_of_location_spelling_errors, number_of_location_stylistic_errors, number_of_incorrect_locations = get_statistics(df_discrepancies)

    # Write a section on the total statistical data
    with open("1res_report.txt", "w", encoding="utf-8") as file:
        file.write("Разминавания в имена на училища от МОН и WikiData\n\n")
        file.write(f"Общ брой разминавания в местоположението: {total_number_of_location_discrepancies}\n")
        file.write(f"Брой правописни грешки в местоположението: {number_of_location_spelling_errors}\n")
        file.write(f"Брой стилистични грешки в местоположението: {number_of_location_stylistic_errors}\n")
        file.write(f"Брой фактологични грешки в местопложението: {number_of_incorrect_locations}\n\n")

        # Write detailed sections with school data
        write_section_to_text_file(file, "Училища с грешки в местоположението", df_discrepancies.loc[df_discrepancies['spelling_error'] == True])
        write_section_to_text_file(file, "Училища с стилистични грешки в местоположението", df_discrepancies.loc[df_discrepancies['stylistic_error'] == True])
        write_section_to_text_file(file, "Училища с фактологични грешки в местоположението", df_discrepancies.loc[df_discrepancies['factual_error'] == True])


def generate_report_pdf(df):
    # Get statistical data
    df_discrepancies = df.loc[df['location_wikidata'] != df['location_mes']]
    df_discrepancies, total_number_of_location_discrepancies, number_of_location_spelling_errors, number_of_location_stylistic_errors, number_of_incorrect_locations = get_statistics(df_discrepancies)
    
    pdf = FPDF()
    pdf.add_page()
    
    # Set font 
    # http://dejavu.svn.sourceforge.net/viewvc/dejavu/trunk/dejavu-fonts/langcover.txt
    FONT_PATH = 'C:\\Users\\ASUS\\Desktop\\DejaVuSansCondensed.ttf'
    pdf.add_font('DejaVu', '', FONT_PATH, uni=True)
    
    pdf.set_font('DejaVu', '', 16)

    # Add title
    pdf.cell(200, 10, txt="Разминавания в имена на училища от МОН и WikiData", ln=True, align='C')
    pdf.ln(10)

    # Add statistics to PDF
    pdf.set_font('DejaVu', '', 12)
    pdf.cell(200, 10, txt="Общ брой разминавания в местоположението: {}".format(total_number_of_location_discrepancies), ln=True)
    pdf.cell(200, 10, txt="Брой правописни грешки в местоположението: {}".format(number_of_location_spelling_errors), ln=True)
    pdf.cell(200, 10, txt="Брой стилистични грешки в местоположението: {}".format(number_of_location_stylistic_errors), ln=True)
    pdf.cell(200, 10, txt="Брой фактологични грешки в местопложението: {}".format(number_of_incorrect_locations), ln=True)
    pdf.ln(10)

    # Add separate sections with detailed data on the schools
    write_section_to_pdf(pdf, "Училища с грешки в местоположението", df_discrepancies.loc[df_discrepancies['spelling_error'] == True])
    write_section_to_pdf(pdf, "Училища с стилистични грешки в местоположението", df_discrepancies.loc[df_discrepancies['stylistic_error'] == True])
    write_section_to_pdf(pdf, "Училища с фактологични грешки в местоположението", df_discrepancies.loc[df_discrepancies['factual_error'] == True])

    # # Save the PDF
    pdf.output("res_report.pdf")
    print(total_number_of_location_discrepancies, number_of_location_spelling_errors, number_of_location_stylistic_errors, number_of_incorrect_locations)


# Merge MES data with WikiData data
df_WikiData = process_data_from_WikiData()
df_MES = process_data_from_MES()
df = df_WikiData.merge(df_MES, on="id", how="inner")

# Generate text report
generate_report_pdf(df)



