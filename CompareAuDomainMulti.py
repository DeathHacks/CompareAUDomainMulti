# BEFORE RUNNING SCRIPT FOR FIRST TIME: 
# pip install -r requirements.txt
#
# run with: 
# python .\CompareAuDomainMulti.py --domain PATH_TO_FILE
#
# Download daily expired domains which are to be purged (https://identitydigital.au/domain-drop-lists/expired) and compare against  Domain Master list file to find matching, generates xlsx with matches, owners and date. 
# Author: DeathHacks
# 
# 
###
###

#!/usr/bin/python3
import argparse
from datetime import date
import pandas as pd 
import warnings
import gc
from concurrent.futures import ThreadPoolExecutor 
import sys
#Ignore warnings for future depreciation
warnings.simplefilter(action='ignore', category=FutureWarning)

#Imports  daily drop list (EXTERNAL) and supplied xlsx into dataframes df1 and df2 respectively
def importdrop():
    print("Importing Domain Drop List")
    expire = "https://identitydigital.au/domain-drop-lists/expired"
    df1 = pd.read_csv(expire)  # read file, file not saved to allow for better memory management
    # Get rid of spaces in column headers
    df1.columns = df1.columns.str.replace(" ", "")
    df2 = pd.read_excel(domain)
    df2.columns = df2.columns.str.replace(" ", "")
    # Return both values to main function  
    return df1, df2

def adjust_domain(df1,df2):
# Remove spaces from headers
    # Append _file1 to all headers
    df1.rename(columns=lambda x: x + "_file1", inplace=True)
    # Change header to match incoming file
    df2.rename(columns={'Domain': 'DomainName'}, inplace=True)
    # Append _file2 to all headers
    df2.rename(columns=lambda x: x + "_file2", inplace=True)
    # Concat both files into one on file column
    df_join = pd.concat([df1, df2], axis=1) 
    # Return df_join to main function
    return df_join

def compare_domains(domain1, domain2):
    # Compare two domains and return a dictionary containing the matching domain and the attributes from the associated dataframe.
    # Access within main function via result['KEY']
    if domain1['DomainName_file1'] == domain2['DomainName_file2']:
        result = ({'Domain': domain1['DomainName_file1'], 
            'Owner': domain2['Owner_file2'],
            'Time': domain1['EligiblePurgeTime_file1'],
            'Date': domain1['Date_file1']
        })
        return result
    else:
        return None

      
# Function to export results. Checks length of matchDomain array, if 0 then all domains have been checked and no matches found.  If > 0 then matches have been found and exported to xlsx in c:\temp   
def export_results():
        if len(matchDomain) == 0:  
            print("All Domains checked, no matches found.")
            sys.exit()
        elif len(matchDomain) > 0:
            print("Matches domains have been found for drop, exporting to c:\temp")
            # Array to dataframe
            matchExcel = pd.DataFrame(matchDomain)
            # Write to excel c:\temp\expiringdomainddmmyy.xlsx
            matchExcel.to_excel('C:\Temp\expiringdomain'+formatdate+'.xlsx', index=False)
            print("Export Complete.  Please check C:\Temp\expiringdomain"+formatdate+".xlsx for results")
            sys.exit()




if __name__ == "__main__":
    parser = argparse.ArgumentParser() #Parse strings provided when executing from command line 
    parser.add_argument("--domain", help="Please specify a xls or xlsx file. Ensure that the file contains the row header 'Domain' , ensuring that all domains are one to a row in the Domain column", required=True) #Add argument to allow for file to be specified from CLI
    args = parser.parse_args()
    domain = args.domain #Declare variable for domain argument. assign provided file path to variable
    
    #Function to download drop list, return Dataframe and assign to variable df1, truple used when calling to return both df1 and 2 and assign to appropiate value 
    df1,df2 = importdrop()
    print("Import Complete, adjusting fields to allow for comparison")
    # Function to make adjustments to data to allow for compare. 
    df_join = adjust_domain(df1,df2)
    print("Adjustment complete! Looking for known Owned Domains within Drop list")
    # Call function to comapre dataframes looking for matching domains. Writes matching domains to keypair and exports to xlsx
    # Define various variables used for loop and date
    dfcolumn1 = len(df1)
    dfcolumn2 = len(df2)
    today = date.today()
    # Set dateformat ddmmYY
    formatdate = str(today.strftime("%d%m%Y"))
    # Create array
    matchDomain = []
    i=0
    # Use ThreadPoolExecutor to parallelize the domain comparison. max_workers set to 100, however can be adjusted.
    with ThreadPoolExecutor(max_workers=1000) as executor:
        #For length of df1, loop through each domain and compare against df2
        for i in range(dfcolumn1):
            domain1 = df_join.loc[i]
            for x in range(dfcolumn2):
                # Assign domain1 and domain2 to variables using df_join.loc. This allows for referencing of single value within colum of dataframe
                domain2 = df_join.loc[x]
                # Submit the compare_domains function to the executor.  If the future is done, assign the result to a variable and print the result.
                future = executor.submit(compare_domains, domain1, domain2)
                # Wait for the future to complete. Check result, if not None then append to matchDomain array
                result = future.result()
                if result is not None:
                    # Appends result to matchDomain. Use of key pair allows for structure export to xlsx. result[KEY] to assess returned value
                    matchDomain.append({
                        'Domain': result['Domain'],'Owner':result['Owner'], 'Time': result['Time'], 'Date': result['Date']
                        })
            x += 1
        i += 1
        #Clean up memory     
        gc.collect()
        # Check if i is equal to dfcolumn1 (length of drop list). if so , all domains checked, export results will then check length of matchDomain to determine if any matches are present. If so export to c:\temp 
        export_results() 
   
