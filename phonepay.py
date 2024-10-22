# Importing necessary libraries
import streamlit as st # Streamlit is used for creating web applications directly from Python
from streamlit_option_menu import option_menu # option_menu is used to create a menu for navigation in Streamlit apps
import psycopg2 # psycopg2 is used for connecting and interacting with PostgreSQL databases
import pandas as pd # pandas is used for data manipulation and analysis, particularly for DataFrames
import plotly.express as px # plotly.express is a high-level interface for creating visualizations
import requests
import json
from PIL import Image

# Dataframe Creation

#sql connection
# Establish a connection to the PostgreSQL database.
# Parameters:
# - host: The address of the database server (in this case, it's the local machine "localhost").
# - user: The username to authenticate with the PostgreSQL server (here, it's "postgres").
# - port: The port number PostgreSQL is running on (default is 5432).
# - database: The name of the database you want to connect to ("phonepe_data").
# - password: The password associated with the specified user (here, "Nanu_300119").
mydb= psycopg2.connect(host= "localhost",
                       user= "postgres",
                       port= "5432",
                       database= "phonepe_data",
                       password= "Nanu_300119")

# Create a cursor object using the connection.
# The cursor allows you to execute PostgreSQL commands and queries through Python.
cursor= mydb.cursor()
# You can now use `cursor` to execute SQL queries and interact with the database.

#aggre_insurance_df
# Execute a SQL query using the cursor to select all records from the 'aggregated_insurance' table.
# The SQL command "SELECT * FROM aggregated_insurance" retrieves all the columns and rows from the table.
cursor.execute("SELECT * FROM aggregated_insurance")
# Commit the current transaction. In this case, the SELECT query doesn't modify the data, 
# so the commit may not be necessary. However, it's a good habit to use commit when you expect data changes 
# like INSERT, UPDATE, DELETE. In this case, it's harmless.
mydb.commit()
# Fetch all the rows returned by the executed SQL query and store them in the variable `table1`.
# `cursor.fetchall()` retrieves all rows as a list of tuples. Each tuple represents a row in the table.
table1= cursor.fetchall()
# Now that we have the data in `table1`, we convert it into a pandas DataFrame for easier analysis.
# pd.DataFrame takes two arguments:
# - `data`: This is the list of tuples (`table1`) that contains the fetched data from the database.
# - `columns`: This defines the column names for the DataFrame. Each tuple in `table1` is expected to
#   match this list of column names in length and order.
Aggre_insurance= pd.DataFrame(table1, columns=("States", "Years", "Quarter", "Transaction_type",
                                               "Transaction_count", "Transaction_amount"))
# Now the DataFrame `Aggre_insurance` contains the data from the 'aggregated_insurance' table 
# with the specified column names.

#aggre_transaction_df
# Execute a SQL query to select all records from the 'aggregated_transaction' table.
# The SQL command "SELECT * FROM aggregated_transaction" retrieves all the columns and rows from this table.
cursor.execute("SELECT * FROM aggregated_transaction")
# Commit the current transaction.
# Even though a SELECT statement doesn't modify data, it's good practice to use commit in other cases where data might be changed (INSERT, UPDATE, DELETE).
mydb.commit()
# Fetch all the rows returned by the executed SQL query and store them in the variable `table2`.
# `cursor.fetchall()` retrieves all rows as a list of tuples. Each tuple represents a row in the table.
table2= cursor.fetchall()
# Now that we have the data in `table2`, we convert it into a pandas DataFrame for easier analysis.
# pd.DataFrame takes two arguments:
# - `data`: This is the list of tuples (`table2`) that contains the fetched data from the database.
# - `columns`: This defines the column names for the DataFrame. The number and order of columns must match the data in `table2`.
Aggre_transaction= pd.DataFrame(table2, columns=("States", "Years", "Quarter", "Transaction_type",
                                               "Transaction_count", "Transaction_amount")) # Column names for the DataFrame.
# Now the DataFrame `Aggre_transaction` contains the data from the 'aggregated_transaction' table 
# with the specified column names.

#aggre_user_df
# Execute a SQL query to select all records from the 'aggregated_user' table.
# The SQL command "SELECT * FROM aggregated_user" retrieves all the columns and rows from this table.
cursor.execute("SELECT * FROM aggregated_user")
# Commit the current transaction.
# For SELECT statements, committing is not necessary since no data is being modified.
# However, commit ensures that if any transaction modifies data, those changes are saved in the database.
mydb.commit()
# Fetch all the rows returned by the executed SQL query and store them in the variable `table3`.
# `cursor.fetchall()` retrieves all rows as a list of tuples. Each tuple represents a row in the table.
table3= cursor.fetchall()
# Now that we have the data in `table3`, we convert it into a pandas DataFrame for easier manipulation and analysis.
# pd.DataFrame takes two arguments:
# - `data`: This is the list of tuples (`table3`) that contains the fetched data from the database.
# - `columns`: This defines the column names for the DataFrame. The number and order of columns must match the data in `table3`.
Aggre_user= pd.DataFrame(table3, columns=("States", "Years", "Quarter", "Brands",
                                               "Transaction_count", "Percentage")) # Column names for the DataFrame.
# The DataFrame `Aggre_user` now holds the data from the 'aggregated_user' table, 
# with labeled columns: "States", "Years", "Quarter", "Brands", "Transaction_count", and "Percentage".

#map_insurance
# Execute a SQL query to select all records from the 'map_insurance' table.
# The SQL command "SELECT * FROM map_insurance" retrieves all the columns and rows from this table.
cursor.execute("SELECT * FROM map_insurance")
# Commit the current transaction.
# Even though a SELECT statement does not change data, commit ensures that any previous transactions modifying data (if any) are saved.
mydb.commit()
# Fetch all the rows returned by the executed SQL query and store them in the variable `table4`.
# `cursor.fetchall()` retrieves all rows as a list of tuples. Each tuple represents a row in the table.
table4= cursor.fetchall()
# Convert the list of tuples (`table4`) into a pandas DataFrame for easier data analysis and manipulation.
# pd.DataFrame takes two arguments:
# - `data`: This is the list of tuples (in this case `table4`) that contains the fetched data from the database.
# - `columns`: This defines the column names for the DataFrame. The number and order of columns must match the structure of the data.
map_insurance= pd.DataFrame(table4, columns=("States", "Years", "Quarter", "District",
                                               "Transaction_count", "Transaction_amount")) # Column names for the DataFrame.
# Now the DataFrame `map_insurance` contains the data from the 'map_insurance' table 
# with labeled columns: "States", "Years", "Quarter", "District", "Transaction_count", and "Transaction_amount".

#map_transction
# Execute a SQL query to select all records from the 'map_transaction' table.
# The SQL command "SELECT * FROM map_transaction" retrieves all the columns and rows from this table.
cursor.execute("SELECT * FROM map_transaction")
# Commit the current transaction.
# Even though a SELECT statement doesn't modify data, commit ensures any previous data-modifying transactions 
# (such as INSERT or UPDATE) are saved. In this case, it's more of a safe practice.
mydb.commit()
# Fetch all the rows returned by the executed SQL query and store them in the variable `table5`.
# `cursor.fetchall()` retrieves all rows as a list of tuples. Each tuple represents a row in the table.
table5= cursor.fetchall()
# Convert the list of tuples (`table5`) into a pandas DataFrame for easier manipulation and analysis.
# pd.DataFrame takes two arguments:
# - `data`: This is the list of tuples (in this case `table5`) containing the fetched data.
# - `columns`: This defines the column names for the DataFrame. The number and order of columns must match the data fetched.
map_transaction= pd.DataFrame(table5, columns=("States", "Years", "Quarter", "District",
                                               "Transaction_count", "Transaction_amount")) # Column names for the DataFrame.
# The DataFrame `map_transaction` now holds the data from the 'map_transaction' table with labeled columns:
# "States", "Years", "Quarter", "District", "Transaction_count", and "Transaction_amount".

#map_user
# Execute a SQL query to select all records from the 'map_user' table.
# The SQL command "SELECT * FROM map_user" retrieves all columns and rows from this table.
cursor.execute("SELECT * FROM map_user")
# Commit the current transaction.
mydb.commit()
# Fetch all the rows returned by the executed SQL query and store them in the variable `table6`.
# `cursor.fetchall()` retrieves all rows as a list of tuples. Each tuple represents a row in the table.
table6= cursor.fetchall() 
# Data fetched from the database (list of tuples).
map_user= pd.DataFrame(table6, columns=("States", "Years", "Quarter", "District",
                                               "RegisteredUser", "AppOpens")) # Column names for the DataFrame.
# The DataFrame `map_user` now holds the data from the 'map_user' table with labeled columns:
# "States", "Years", "Quarter", "District", "RegisteredUser", and "AppOpens".

#top_insurance
# Execute a SQL query to select all records from the 'top_insurance' table.
# The SQL command "SELECT * FROM top_insurance" retrieves all the columns and rows from this table.
cursor.execute("SELECT * FROM top_insurance")
# Commit the current transaction.
mydb.commit()
# Fetch all the rows returned by the executed SQL query and store them in the variable `table7`.
# `cursor.fetchall()` retrieves all rows as a list of tuples. Each tuple represents a row in the table.
table7= cursor.fetchall()
# Convert the list of tuples (`table7`) into a pandas DataFrame for easier analysis.
top_insurance= pd.DataFrame(table7, columns=("States", "Years", "Quarter", "Pincodes",
                                               "Transaction_count", "Transaction_amount"))
# The DataFrame `top_insurance` now holds the data from the 'top_insurance' table with labeled columns:
# "States", "Years", "Quarter", "Pincodes", "Transaction_count", and "Transaction_amount".

#top_transaction
# Execute a SQL query to select all records from the 'top_transaction' table.
# The SQL command "SELECT * FROM top_transaction" retrieves all columns and rows from this table.
cursor.execute("SELECT * FROM top_transaction")
# Commit the current transaction.
mydb.commit()
# Fetch all the rows returned by the executed SQL query and store them in the variable `table8`.
# `cursor.fetchall()` retrieves all rows as a list of tuples. Each tuple represents a row in the table.
table8= cursor.fetchall()
# Convert the list of tuples (`table8`) into a pandas DataFrame for easier analysis.
# Data fetched from the database (list of tuples).
top_transaction= pd.DataFrame(table8, columns=("States", "Years", "Quarter", "Pincodes",
                                               "Transaction_count", "Transaction_amount")) # Column names for the DataFrame.
# The DataFrame `top_transaction` now holds the data from the 'top_transaction' table with labele

#top_user
# Execute a SQL query to select all records from the 'top_user' table.
# The SQL command "SELECT * FROM top_user" retrieves all columns and rows from this table.
cursor.execute("SELECT * FROM top_user")
# Commit the current transaction.
mydb.commit()
# Fetch all rows from the result.
table9= cursor.fetchall()
# Convert the result into a pandas DataFrame with specified column names.
# Data fetched from the database.
top_user= pd.DataFrame(table9, columns=("States", "Years", "Quarter", "Pincodes",
                                               "RegisteredUsers")) # Column names for the DataFrame.
# The DataFrame `top_user` now holds the data from the 'top_user' table with labeled columns:
# "States", "Years", "Quarter", "Pincodes", and "RegisteredUsers".


def Transaction_amount_count_Y(df, year): 
    # Purpose: Analyze and visualize transaction data for a specific year.

    # Filter the DataFrame to include only the rows for the specified year.
    tacy= df[df["Years"] == year]

    # Reset the index of the filtered DataFrame to have a clean index starting from 0.
    tacy.reset_index(drop = True, inplace= True)

    # Group the filtered DataFrame by 'States' and calculate the sum of 'Transaction_count' and 'Transaction_amount'.
    tacyg= tacy.groupby("States")[["Transaction_count","Transaction_amount"]].sum()

    # Reset the index of the grouped DataFrame to convert the 'States' index back into a column.
    tacyg.reset_index(inplace= True)

    # Create two columns in the Streamlit app layout for displaying the bar charts.
    col1,col2 = st.columns(2)
    # Create and display the bar chart for total transaction amount in the first column.
    with col1:
        # Creating a bar chart for Transaction Amounts
        fig_amount= px.bar(tacyg, x="States", y="Transaction_amount", title=f"{year} TRANSACTION AMOUNT",
                        color_discrete_sequence=px.colors.sequential.Aggrnyl, height= 650,width= 600)
        # Render the bar chart in the Streamlit app.
        st.plotly_chart(fig_amount)

    # Create and display the bar chart for total transaction count in the second column.
    with col2:
        # Creating a bar chart for Transaction Counts
        fig_count= px.bar(tacyg, x="States", y="Transaction_count", title=f"{year} TRANSACTION COUNT",
                        color_discrete_sequence=px.colors.sequential.Bluered_r, height= 650, width= 600)
        # Render the transaction count bar chart in the Streamlit app.
        st.plotly_chart(fig_count)


    col1,col2= st.columns(2)
    # Create a choropleth map for transaction amount in the first column.
    with col1:
        # Fetch GeoJSON data for Indian states.
        url= "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response= requests.get(url) # Make an HTTP GET request to fetch the GeoJSON data.
        data1= json.loads(response.content) # Load the response content into a JSON format.
        # Extract the names of states from the GeoJSON features.
        states_name= []
        for feature in data1["features"]:
            states_name.append(feature["properties"]["ST_NM"])

        # Sort the list of states alphabetically.
        states_name.sort()

        # Create a choropleth map for transaction amounts using Plotly Express.
        fig_india_1= px.choropleth(tacyg, geojson= data1, locations= "States", featureidkey= "properties.ST_NM",
                                color= "Transaction_amount", color_continuous_scale= "Rainbow",
                                range_color= (tacyg["Transaction_amount"].min(), tacyg["Transaction_amount"].max()),
                                hover_name= "States", title= f"{year} TRANSACTION AMOUNT", fitbounds= "locations",
                                height= 600,width= 600)
        fig_india_1.update_geos(visible= False) # Hide the geographical borders for a cleaner look.
        # Render the choropleth map for transaction amounts in the Streamlit app.
        st.plotly_chart(fig_india_1)

    # Create a choropleth map for transaction count in the second column.
    with col2:
        # Creating a choropleth map for Transaction Counts
        fig_india_2= px.choropleth(tacyg, geojson= data1, locations= "States", featureidkey= "properties.ST_NM",
                                color= "Transaction_count", color_continuous_scale= "Rainbow",
                                range_color= (tacyg["Transaction_count"].min(), tacyg["Transaction_count"].max()),
                                hover_name= "States", title= f"{year} TRANSACTION COUNT", fitbounds= "locations",
                                height= 600,width= 600)
        fig_india_2.update_geos(visible= False) # Hide the geographical borders for a cleaner look.
        # Render the choropleth map for transaction counts in the Streamlit app.
        st.plotly_chart(fig_india_2)
    # Return the filtered DataFrame containing transaction details for the specified year.
    return tacy

# Function to analyze and visualize transaction amount and count by states for a given quarter
def Transaction_amount_count_Y_Q(df, quarter):

    # Filter the DataFrame for the given quarter
    tacy= df[df["Quarter"] == quarter]

    # Reset the index of the filtered DataFrame to ensure continuous indexing
    tacy.reset_index(drop = True, inplace= True)

    # Group the data by 'States' and sum the 'Transaction_count' and 'Transaction_amount' for each state
    tacyg= tacy.groupby("States")[["Transaction_count","Transaction_amount"]].sum()

    # Reset the index of the grouped DataFrame to allow plotting and other operations
    tacyg.reset_index(inplace= True)

    # Create two columns for side-by-side visualization using Streamlit
    col1,col2= st.columns(2)

    # First visualization: Bar chart for Transaction Amount by State
    with col1:

        # Create a bar chart using Plotly to display Transaction Amount by States
        fig_amount= px.bar(tacyg, x="States", y="Transaction_amount", title=f"{tacy['Years'].min()} YEAR {quarter} QUARTER TRANSACTION AMOUNT",
                        color_discrete_sequence=px.colors.sequential.Aggrnyl, height= 650,width= 600) # Color scheme # Size of the chart
        
        # Display the bar chart in Streamlit
        st.plotly_chart(fig_amount)

    # Second visualization: Bar chart for Transaction Count by State
    with col2:
        # Create a bar chart using Plotly to display Transaction Count by States
        fig_count= px.bar(tacyg, x="States", y="Transaction_count", title=f"{tacy['Years'].min()} YEAR {quarter} QUARTER TRANSACTION COUNT",
                        color_discrete_sequence=px.colors.sequential.Bluered_r, height= 650,width= 600) # Color scheme # Size of the chart
        
        # Display the bar chart in Streamlit
        st.plotly_chart(fig_count)

    # Create another set of two columns for the choropleth maps
    col1,col2= st.columns(2)
    # First choropleth map: Visualizing Transaction Amount across Indian states
    with col1:
        # URL for geojson data of Indian states
        url= "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        # Request the geojson data from the URL
        response= requests.get(url)
        # Load the geojson data as a Python dictionary
        data1= json.loads(response.content)

        # Extract the list of Indian state names from the geojson data
        states_name= []
        for feature in data1["features"]:
            states_name.append(feature["properties"]["ST_NM"])

        # Sort the state names alphabetically
        states_name.sort()

        # Create a choropleth map using Plotly to display Transaction Amount by state
        fig_india_1= px.choropleth(tacyg, geojson= data1, locations= "States", featureidkey= "properties.ST_NM",
                                color= "Transaction_amount", color_continuous_scale= "Rainbow",
                                range_color= (tacyg["Transaction_amount"].min(), tacyg["Transaction_amount"].max()),
                                hover_name= "States", title= f"{tacy['Years'].min()} YEAR {quarter} QUARTER TRANSACTION AMOUNT", fitbounds= "locations",
                                height= 600,width= 600) # Map states in geojson to DataFrame # Color scale for the map # Set the color range # Fit map bounds to data # Size of the map
        # Hide the default geographic features (like coastlines, etc.)                                                
        fig_india_1.update_geos(visible= False)

        # Display the choropleth map in Streamlit
        st.plotly_chart(fig_india_1)
    # Second choropleth map: Visualizing Transaction Count across Indian states
    with col2:
        # Create a choropleth map using Plotly to display Transaction Count by state
        fig_india_2= px.choropleth(tacyg, geojson= data1, locations= "States", featureidkey= "properties.ST_NM",
                                color= "Transaction_count", color_continuous_scale= "Rainbow",
                                range_color= (tacyg["Transaction_count"].min(), tacyg["Transaction_count"].max()),
                                hover_name= "States", title= f"{tacy['Years'].min()} YEAR {quarter} QUARTER TRANSACTION COUNT", fitbounds= "locations",
                                height= 600,width= 600)
        # Hide the default geographic features (like coastlines, etc.)
        fig_india_2.update_geos(visible= False)
        # Display the choropleth map in Streamlit
        st.plotly_chart(fig_india_2)
    # Return the filtered and grouped DataFrame for further use if needed
    return tacy

# Function to analyze and visualize transaction amount and count by transaction type for a given state            
def Aggre_Tran_Transaction_type(df, state):

    # Filter the DataFrame to only include data for the specified state
    tacy= df[df["States"] == state]

    # Reset the index of the filtered DataFrame for clean indexing
    tacy.reset_index(drop = True, inplace= True)

    # Group the data by 'Transaction_type' and sum the 'Transaction_count' and 'Transaction_amount' for each transaction type
    tacyg= tacy.groupby("Transaction_type")[["Transaction_count","Transaction_amount"]].sum()
    # Reset the index of the grouped DataFrame to prepare it for visualization
    tacyg.reset_index(inplace= True)

    # Create two columns for side-by-side pie charts using Streamlit
    col1,col2= st.columns(2)
    # First visualization: Pie chart for Transaction Amount by Transaction Type
    with col1:
        # Create a pie chart using Plotly to display the proportion of Transaction Amount by Transaction Type
        fig_pie_1= px.pie(data_frame= tacyg, names= "Transaction_type", values= "Transaction_amount", # Categories for the pie chart # Values for the pie chart
                            width= 600, title= f"{state.upper()} TRANSACTION AMOUNT", hole= 0.5) # Set the width of the chart # Title for the chart # Creates a donut-shaped chart by adding a hole in the center
        # Display the pie chart in Streamlit
        st.plotly_chart(fig_pie_1)

    # Second visualization: Pie chart for Transaction Count by Transaction Type
    with col2:
        # Create a pie chart using Plotly to display the proportion of Transaction Count by Transaction Type
        fig_pie_2= px.pie(data_frame= tacyg, names= "Transaction_type", values= "Transaction_count", # Categories for the pie chart # Values for the pie chart
                            width= 600, title= f"{state.upper()} TRANSACTION COUNT", hole= 0.5) # Set the width of the chart # Title for the chart # Creates a donut-shaped chart by adding a hole in the center
        # Display the pie chart in Streamlit
        st.plotly_chart(fig_pie_2)


# Aggre_User_analysis_1
# Define the function to plot a bar chart based on the user's transaction data for a given year
def Aggre_user_plot_1(df, year):
    # Step 1: Filter the DataFrame to only include data for the specified year
    # We are selecting rows where the 'Years' column matches the given year
    aguy= df[df["Years"]== year]
    # Step 2: Reset the index of the filtered DataFrame
    # This ensures the index starts from 0, making it easier to handle later on
    # 'drop=True' means the old index is discarded instead of being added as a new column
    aguy.reset_index(drop= True, inplace= True)

    # Step 3: Group the data by the 'Brands' column and calculate the sum of 'Transaction_count' for each brand
    # This gives us the total number of transactions for each brand in the selected year
    aguyg= pd.DataFrame(aguy.groupby("Brands")["Transaction_count"].sum())
    # Step 4: Reset the index of the grouped DataFrame to flatten the structure
    # This turns the 'Brands' from an index back into a regular column
    aguyg.reset_index(inplace= True)

    # Step 5: Create a bar chart using Plotly Express
    # The x-axis will be the different brands and the y-axis will represent the total transaction count
    # 'title' provides the chart title
    # 'color_discrete_sequence' changes the bar colors to use a predefined color palette
    # 'hover_name' ensures that hovering over the bars will display the brand names
    fig_bar_1= px.bar(aguyg, x= "Brands", y= "Transaction_count", title= f"{year} BRANDS AND TRANSACTION COUNT",
                    width= 1000, color_discrete_sequence= px.colors.sequential.haline_r, hover_name= "Brands")
    # Step 6: Render the plot using Streamlit's plotly_chart function
    # This displays the plot in the Streamlit app
    st.plotly_chart(fig_bar_1)

    # Step 7: Return the filtered DataFrame (aguy) for further use, if needed
    return aguy

#Aggre_user_Analysis_2
# Define the function to plot a bar chart based on transaction data for a given quarter
def Aggre_user_plot_2(df, quarter):
    # Step 1: Filter the DataFrame to only include data for the specified quarter
    # We are selecting rows where the 'Quarter' column matches the given quarter
    aguyq= df[df["Quarter"]== quarter]
    # Step 2: Reset the index of the filtered DataFrame
    # This ensures the index starts from 0, making it easier to handle later on
    # 'drop=True' means the old index is discarded instead of being added as a new column
    aguyq.reset_index(drop= True, inplace= True)
    # Step 3: Group the data by the 'Brands' column and calculate the sum of 'Transaction_count' for each brand
    # This gives us the total number of transactions for each brand in the selected quarter
    aguyqg= pd.DataFrame(aguyq.groupby("Brands")["Transaction_count"].sum())
    # Step 4: Reset the index of the grouped DataFrame to flatten the structure
    # This turns the 'Brands' from an index back into a regular column
    aguyqg.reset_index(inplace= True)
    # Step 5: Create a bar chart using Plotly Express
    # The x-axis will be the different brands, and the y-axis will represent the total transaction count
    # 'title' provides the chart title
    # 'color_discrete_sequence' changes the bar colors to use a predefined color palette
    # 'hover_name' ensures that hovering over the bars will display the brand names
    fig_bar_1= px.bar(aguyqg, x= "Brands", y= "Transaction_count", title=  f"{quarter} QUARTER, BRANDS AND TRANSACTION COUNT",
                    width= 1000, color_discrete_sequence= px.colors.sequential.Magenta_r, hover_name="Brands")
    # Step 6: Render the plot using Streamlit's plotly_chart function
    # This displays the plot in the Streamlit app
    st.plotly_chart(fig_bar_1)
    # Step 7: Return the filtered DataFrame (aguyq) for further use, if needed
    return aguyq


#Aggre_user_alalysis_3
# Define the function to plot a line chart based on transaction data for a given state
def Aggre_user_plot_3(df, state):
     # Step 1: Filter the DataFrame to only include data for the specified state
    # We are selecting rows where the 'States' column matches the given state
    auyqs= df[df["States"] == state]
    # Step 2: Reset the index of the filtered DataFrame
    # This ensures the index starts from 0, making it easier to handle later on
    # 'drop=True' means the old index is discarded instead of being added as a new column
    auyqs.reset_index(drop= True, inplace= True)
    # Step 3: Create a line chart using Plotly Express
    # The x-axis will be the different brands, and the y-axis will represent the transaction count for each brand
    # 'hover_data' allows us to show additional information when hovering over the data points, in this case, 'Percentage'
    # 'title' sets the chart title and 'width' adjusts the size of the plot
    # 'markers=True' ensures that data points are marked with circles on the line plot
    fig_line_1= px.line(auyqs, x= "Brands", y= "Transaction_count", hover_data= "Percentage", # Display additional information (percentage) when hovering
                        title= f"{state.upper()} BRANDS, TRANSACTION COUNT, PERCENTAGE",width= 1000, markers= True)  # Title with state in uppercase # Show markers (circles) at each data point
    # Step 4: Render the plot using Streamlit's plotly_chart function
    # This displays the line chart in the Streamlit app
    st.plotly_chart(fig_line_1)


#Map_insurance_district
# Define the function to plot horizontal bar charts for transaction count and amount by district in a given state
def Map_insur_District(df, state):
    # Step 1: Filter the DataFrame to only include data for the specified state
    # We are selecting rows where the 'States' column matches the given state
    tacy= df[df["States"] == state]
    # Step 2: Reset the index of the filtered DataFrame
    # This ensures the index starts from 0, making it easier to handle later on
    # 'drop=True' means the old index is discarded instead of being added as a new column
    tacy.reset_index(drop = True, inplace= True)
    # Step 3: Group the data by the 'District' column and calculate the sum of 'Transaction_count' and 'Transaction_amount' for each district
    # This allows us to see the total number of transactions and their amounts per district
    tacyg= tacy.groupby("District")[["Transaction_count","Transaction_amount"]].sum()
    # Step 4: Reset the index of the grouped DataFrame to flatten the structure
    # This turns 'District' from an index back into a regular column
    tacyg.reset_index(inplace= True)
    # Step 5: Split the Streamlit layout into two columns (col1 and col2)
    col1,col2= st.columns(2)
    with col1:
        fig_bar_1= px.bar(tacyg, x= "Transaction_amount", y= "District", orientation= "h", height= 600, # Horizontal bars
                        title= f"{state.upper()} DISTRICT AND TRANSACTION AMOUNT", color_discrete_sequence= px.colors.sequential.Mint_r)    
        # Render the bar chart in Streamlit
        st.plotly_chart(fig_bar_1)
    # Step 7: Create the second bar chart (in col2) showing transaction counts by district
    # Same as above, but here we plot 'Transaction_count'
    with col2:

        fig_bar_2= px.bar(tacyg, x= "Transaction_count", y= "District", orientation= "h", height= 600,  # Horizontal bars
                        title= f"{state.upper()} DISTRICT AND TRANSACTION COUNT", color_discrete_sequence= px.colors.sequential.Bluered_r)
        # Render the second bar chart in Streamlit
        st.plotly_chart(fig_bar_2)

# map_user_plot_1
# Define the function to plot a line chart based on registered user data and app opens for a given year
def map_user_plot_1(df, year):
    # Step 1: Filter the DataFrame to only include data for the specified year
    # We are selecting rows where the 'Years' column matches the given year
    muy= df[df["Years"]== year]
    # Step 2: Reset the index of the filtered DataFrame
    # This ensures the index starts from 0, making it easier to handle later on
    # 'drop=True' means the old index is discarded instead of being added as a new column
    muy.reset_index(drop= True, inplace= True)

    # Step 3: Group the data by the 'States' column and calculate the sum of 'RegisteredUser' and 'AppOpens' for each state
    # This gives us the total number of registered users and app opens for each state in the selected year
    muyg= muy.groupby("States")[["RegisteredUser", "AppOpens"]].sum()
    # Step 4: Reset the index of the grouped DataFrame to flatten the structure
    # This turns 'States' from an index back into a regular column
    muyg.reset_index(inplace= True)
    # Step 5: Create a line chart using Plotly Express
    # The x-axis will be the different states, and the y-axis will represent both registered users and app opens
    # 'markers=True' ensures that data points are marked with circles on the line plot
    fig_line_1= px.line(muyg, x= "States", y= ["RegisteredUser", "AppOpens"],   # Plotting both 'RegisteredUser' and 'AppOpens' # Title includes the selected year
                        title= f"{year} REGISTERED USER, APPOPENS",width= 1000, height= 800, markers= True) # Chart width   # Chart height   # Add markers (circles) at each data point
     # Step 6: Render the plot using Streamlit's plotly_chart function
    # This displays the line chart in the Streamlit app
    st.plotly_chart(fig_line_1)

    # Step 7: Return the filtered DataFrame (muy) for further use, if needed
    return muy

# map_user_plot_2
# Define the function to plot a line chart based on registered user data and app opens for a given quarter
def map_user_plot_2(df, quarter):
    # Step 1: Filter the DataFrame to only include data for the specified quarter
    # We are selecting rows where the 'Quarter' column matches the given quarter
    muyq= df[df["Quarter"]== quarter]
    # Step 2: Reset the index of the filtered DataFrame
    # This ensures the index starts from 0, making it easier to handle later on
    # 'drop=True' means the old index is discarded instead of being added as a new column
    muyq.reset_index(drop= True, inplace= True)
    # Step 3: Group the data by the 'States' column and calculate the sum of 'RegisteredUser' and 'AppOpens' for each state
    # This gives us the total number of registered users and app opens for each state in the selected quarter
    muyqg= muyq.groupby("States")[["RegisteredUser", "AppOpens"]].sum()
    # Step 4: Reset the index of the grouped DataFrame to flatten the structure
    # This turns 'States' from an index back into a regular column
    muyqg.reset_index(inplace= True)
    # Step 5: Create a line chart using Plotly Express
    # The x-axis will be the different states, and the y-axis will represent both registered users and app opens
    # 'markers=True' ensures that data points are marked with circles on the line plot
    fig_line_1= px.line(muyqg, x= "States", y= ["RegisteredUser", "AppOpens"],  # Plotting both 'RegisteredUser' and 'AppOpens'
                        title= f"{df['Years'].min()} YEARS {quarter} QUARTER REGISTERED USER, APPOPENS",width= 1000, height= 800, markers= True, # Title dynamically includes the earliest year and selected quarter
                        color_discrete_sequence= px.colors.sequential.Rainbow_r)    # Chart width   # Chart height  # Add markers (circles) at each data point  # Use the 'Rainbow_r' color scheme
    # Step 6: Render the plot using Streamlit's plotly_chart function
    # This displays the line chart in the Streamlit app
    st.plotly_chart(fig_line_1)
    # Step 7: Return the filtered DataFrame (muyq) for further use, if needed
    return muyq

#map_user_plot_3
# Define the function to plot bar charts for registered users and app opens by district for a given state
def map_user_plot_3(df, states):
    # Step 1: Filter the DataFrame to only include data for the specified state
    # We are selecting rows where the 'States' column matches the given state
    muyqs= df[df["States"]== states]
    # Step 2: Reset the index of the filtered DataFrame
    # This ensures the index starts from 0, making it easier to handle later on
    # 'drop=True' means the old index is discarded instead of being added as a new column
    muyqs.reset_index(drop= True, inplace= True)
    # Step 3: Split the Streamlit layout into two columns (col1 and col2)
    # This creates a side-by-side layout where one chart will be displayed in each column
    col1,col2= st.columns(2)
    # Step 4: Create the first bar chart (in col1) showing registered users by district
    # 'orientation="h"' makes the bar chart horizontal
    # 'color_discrete_sequence' defines a color scheme for the bars (Rainbow_r)
    with col1:
        fig_map_user_bar_1= px.bar(muyqs, x= "RegisteredUser", y= "District", orientation= "h", # Horizontal bars
                                title= f"{states.upper()} REGISTERED USER", height= 800, color_discrete_sequence= px.colors.sequential.Rainbow_r)   # Title includes state in uppercase # Use a reverse rainbow color schem
        # Render the first bar chart in Streamlit
        st.plotly_chart(fig_map_user_bar_1)
    # Step 5: Create the second bar chart (in col2) showing app opens by district
    # Same structure as the first chart, but plotting 'AppOpens' instead
    with col2:

        fig_map_user_bar_2= px.bar(muyqs, x= "AppOpens", y= "District", orientation= "h",   # Horizontal bars   # Title includes state in uppercase
                                title= f"{states.upper()} APPOPENS", height= 800, color_discrete_sequence= px.colors.sequential.Rainbow) # Chart height # Use a rainbow color scheme
        # Render the second bar chart in Streamlit
        st.plotly_chart(fig_map_user_bar_2)

# top_insurance_plot_1
# Define a function to plot transaction amount and transaction count by quarter for a specific state
def Top_insurance_plot_1(df, state):
    # Step 1: Filter the DataFrame to include data only for the specified state
    tiy= df[df["States"]== state]
    # Step 2: Reset the index of the filtered DataFrame
    # This ensures that the DataFrame is indexed from 0, making it easier to work with
    tiy.reset_index(drop= True, inplace= True)
    # Step 3: Create two columns in the layout using Streamlit's column functionality
    col1,col2= st.columns(2)
    # Step 4: In the first column, create a bar chart for the transaction amount by quarter
    with col1:
        fig_top_insur_bar_1= px.bar(tiy, x= "Quarter", y= "Transaction_amount", hover_data= "Pincodes", # X-axis: Quarter # Y-axis: Transaction amount # Show Pincodes when hovering over the bars   
                                title= "TRANSACTION AMOUNT", height= 650,width= 600, color_discrete_sequence= px.colors.sequential.GnBu_r) # Chart title # Height of the chart # Width of the chart # Color scheme for the bars (GnBu_r color palette) 
        # Step 5: Display the first bar chart in Streamlit
        st.plotly_chart(fig_top_insur_bar_1)
    # Step 6: In the second column, create a bar chart for the transaction count by quarter
    with col2:

        fig_top_insur_bar_2= px.bar(tiy, x= "Quarter", y= "Transaction_count", hover_data= "Pincodes", # X-axis: Quarter # Y-axis: Transaction count # Show Pincodes when hovering over the bars    
                                title= "TRANSACTION COUNT", height= 650,width= 600, color_discrete_sequence= px.colors.sequential.Agsunset_r) # Chart title # Height of the chart # Width of the chart # Color scheme for the bars (Agsunset_r color palette)
        # Step 7: Display the second bar chart in Streamlit
        st.plotly_chart(fig_top_insur_bar_2)

# Define a function to plot registered users by state and quarter for a given year
def top_user_plot_1(df, year):
    # Step 1: Filter the DataFrame to include only data for the specified year
    # This filters rows where the 'Years' column matches the provided 'year' parameter
    tuy= df[df["Years"]== year]
    # Step 2: Reset the index of the filtered DataFrame
    # This removes the old index and reassigns a new one starting from 0
    tuy.reset_index(drop= True, inplace= True)
    # Step 3: Group the data by 'States' and 'Quarter', then sum the 'RegisteredUsers'
    # This creates a new DataFrame 'tuyg' where the total number of registered users
    # for each combination of state and quarter is calculated
    tuyg= pd.DataFrame(tuy.groupby(["States", "Quarter"])["RegisteredUsers"].sum())
    # Step 4: Reset the index of the grouped DataFrame
    # This ensures the DataFrame is flat (not multi-indexed), making it easier to plot
    tuyg.reset_index(inplace= True)
    # Step 5: Create a bar chart using Plotly Express
    # The X-axis represents 'States', and the Y-axis represents 'RegisteredUsers'
    # The bars are color-coded by 'Quarter' to distinguish between different quarters
    fig_top_plot_1= px.bar(tuyg, x= "States", y= "RegisteredUsers", color= "Quarter", width= 1000, height= 800, # X-axis: States # Y-axis: Registered Users 
                        color_discrete_sequence= px.colors.sequential.Burgyl, hover_name= "States",  # Color-code the bars by quarter # Width of the chart # Height of the chart
                        title= f"{year} REGISTERED USERS")# Color scheme (Burgyl palette) # Show state name when hovering over the bars # Title of the chart with the year
    # Step 6: Display the bar chart in the Streamlit app
    st.plotly_chart(fig_top_plot_1)
    # Step 7: Return the filtered DataFrame for potential further use
    return tuy


# top_user_plot_2
# Define the function top_user_plot_2 which takes two parameters: 
# 'df' - the dataframe containing the data, and 'state' - the specific state to filter data for.
def top_user_plot_2(df, state):
    # Step 1: Filter the dataframe to include only the rows where the "States" column matches the provided 'state'.
    tuys= df[df["States"]== state]
    # Step 2: Reset the index of the filtered dataframe, 
    # dropping the old index and replacing it with a new one (starting from 0).
    tuys.reset_index(drop= True, inplace= True)
    # Step 3: Create an interactive bar plot using Plotly Express.
    # The plot will visualize the number of Registered Users by Quarter for the specified state.
    # The filtered dataframe to plot
    # X-axis: quarters of the year
    # Y-axis: the number of registered users
    # The title of the plot
    # Width of the plot in pixels
    # Height of the plot in pixels
    # Color bars based on the number of registered users
    # Additional hover info: Pincodes will be shown when hovering over bars
    # Color scale: a magenta-based gradient for the bars
    fig_top_pot_2= px.bar(tuys, x= "Quarter", y= "RegisteredUsers", title= "REGISTEREDUSERS, PINCODES, QUARTER",
                        width= 1000, height= 800, color= "RegisteredUsers", hover_data= "Pincodes",
                        color_continuous_scale= px.colors.sequential.Magenta)
    # Step 4: Display the plot in the Streamlit app using st.plotly_chart.
    # This is specific to Streamlit applications and renders the Plotly chart in the web app.
    st.plotly_chart(fig_top_pot_2)

#sql connection
def top_chart_transaction_amount(table_name):
    # Establish a connection to the PostgreSQL database using psycopg2
    mydb= psycopg2.connect(host= "localhost",           # Database host (local machine)
                        user= "postgres",               # Username for the PostgreSQL database
                        port= "5432",                   # Port number where PostgreSQL is running
                        database= "phonepe_data",       # Name of the database to connect to
                        password= "Nanu_300119")        # Password for the database user
    # Create a cursor object to execute queries
    cursor= mydb.cursor()
    
    # Query to get the top 10 states by transaction amount
    #plot_1
    query1= f'''SELECT states, SUM(transaction_amount) AS transaction_amount
                FROM {table_name}
                GROUP BY states
                ORDER BY transaction_amount DESC
                LIMIT 10;'''
    # Execute the first query
    cursor.execute(query1)
    # Fetch the results of the query
    table_1= cursor.fetchall()
    mydb.commit()
    # Commit the transaction (although it's not strictly necessary for SELECT queries)
    df_1= pd.DataFrame(table_1, columns=("states", "transaction_amount"))
    # Create two columns layout for Streamlit app to display charts side by side
    col1,col2= st.columns(2)
    # Display the first bar chart in the first column
    with col1:
        # X-axis as states
        # Y-axis as the transaction amount
        # Chart title
        # Display state names when hovering over bars
        # Color scheme
        # Height of the chart , Width of the chart
        fig_amount= px.bar(df_1, x="states", y="transaction_amount", title="TOP 10 OF TRANSACTION AMOUNT", hover_name= "states",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl, height= 650,width= 600)
        # Render the chart in Streamlit
        st.plotly_chart(fig_amount)

    #plot_2
    # Query to get the bottom 10 states by transaction amount
    query2= f'''SELECT states, SUM(transaction_amount) AS transaction_amount
                FROM {table_name}
                GROUP BY states
                ORDER BY transaction_amount
                LIMIT 10;'''
    # Execute the second query
    cursor.execute(query2)
    # Fetch the results of the query
    table_2= cursor.fetchall()
    # Commit the transaction
    mydb.commit()
    # Create a DataFrame from the fetched data for visualization
    df_2= pd.DataFrame(table_2, columns=("states", "transaction_amount"))
    # Display the second bar chart in the second column
    with col2:
        # Create a bar plot for bottom 10 transaction amounts
        # Chart title for bottom 10
        # Hover to display state names
        # Reversed color scheme
        # Height of the chart
        # Width of the chart
        fig_amount_2= px.bar(df_2, x="states", y="transaction_amount", title="LAST 10 OF TRANSACTION AMOUNT", hover_name= "states",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl_r, height= 650,width= 600)
        # Render the chart in Streamlit
        st.plotly_chart(fig_amount_2)

    #plot_3
    # Query to get the average transaction amount per state
    query3= f'''SELECT states, AVG(transaction_amount) AS transaction_amount
                FROM {table_name}
                GROUP BY states
                ORDER BY transaction_amount;'''
    # Execute the third query
    cursor.execute(query3)
    # Fetch the results of the query
    table_3= cursor.fetchall()
    # Commit the transaction
    mydb.commit()
    # Create a DataFrame from the fetched data for visualization
    df_3= pd.DataFrame(table_3, columns=("states", "transaction_amount"))
    # Create a horizontal bar plot for average transaction amounts
    # Y-axis as states (for horizontal bars)
    # X-axis as the average transaction amount
    # Chart title , Display state names when hovering ,Horizontal orientation for the bar chart
    # Custom color scheme, Height of the chart,Width of the chart
    fig_amount_3= px.bar(df_3, y="states", x="transaction_amount", title="AVERAGE OF TRANSACTION AMOUNT", hover_name= "states", orientation= "h",
                        color_discrete_sequence=px.colors.sequential.Bluered_r, height= 800,width= 1000)
    # Render the chart in Streamlit
    st.plotly_chart(fig_amount_3)


#sql connection
def top_chart_transaction_count(table_name):
    # Establish a connection to the PostgreSQL database using psycopg2
    mydb= psycopg2.connect(host= "localhost",       # Host where the database server is running
                        user= "postgres",           # Username to authenticate with the database
                        port= "5432",               # Port where PostgreSQL service is listening
                        database= "phonepe_data",   # Name of the database to connect to
                        password= "Nanu_300119")    # Password for the PostgreSQL user
    # Create a cursor object to execute SQL queries
    cursor= mydb.cursor()

    #plot_1
    # Query to fetch the top 10 states with the highest transaction counts
    query1= f'''SELECT states, SUM(transaction_count) AS transaction_count
                FROM {table_name}
                GROUP BY states
                ORDER BY transaction_count DESC
                LIMIT 10;'''
    # Execute the first query
    cursor.execute(query1)
    # Fetch the result of the query and store it in a variable
    table_1= cursor.fetchall()
    # Commit the transaction (optional for SELECT queries)
    mydb.commit()
    # Create a pandas DataFrame from the fetched data
    df_1= pd.DataFrame(table_1, columns=("states", "transaction_count"))
    # Create two columns layout in Streamlit to display charts side by side
    col1,col2= st.columns(2)
    # Display the first bar chart in the first column
    with col1:
        # Create a bar chart using Plotly Express for top 10 states by transaction count
        # X-axis as states, Y-axis as transaction counts
        # Title of the chart, Display state names on hover, Color scheme
        # Height of the chart, Width of the chart
        fig_amount= px.bar(df_1, x="states", y="transaction_count", title="TOP 10 OF TRANSACTION COUNT", hover_name= "states",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl, height= 650,width= 600)
        # Render the chart in the Streamlit app
        st.plotly_chart(fig_amount)

    #plot_2
    # Query to fetch the bottom 10 states with the lowest transaction counts
    query2= f'''SELECT states, SUM(transaction_count) AS transaction_count
                FROM {table_name}
                GROUP BY states
                ORDER BY transaction_count
                LIMIT 10;'''
    # Execute the second query
    cursor.execute(query2)
    # Fetch the result of the query and store it in a variable
    table_2= cursor.fetchall()
    # Commit the transaction
    mydb.commit()
    # Create a pandas DataFrame from the fetched data
    df_2= pd.DataFrame(table_2, columns=("states", "transaction_count"))
    # Display the second bar chart in the second column
    with col2:
        # Create a bar chart using Plotly Express for bottom 10 states by transaction count
        # Title of the chart , Display state names on hover , Reversed color scheme, Height of the chart, Width of the chart
        fig_amount_2= px.bar(df_2, x="states", y="transaction_count", title="LAST 10 OF TRANSACTION COUNT", hover_name= "states",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl_r, height= 650,width= 600)
        # Render the chart in the Streamlit app
        st.plotly_chart(fig_amount_2)

    #plot_3
    # Query to fetch the average transaction count for each state
    query3= f'''SELECT states, AVG(transaction_count) AS transaction_count
                FROM {table_name}
                GROUP BY states
                ORDER BY transaction_count;'''
    # Execute the third query
    cursor.execute(query3)
    # Fetch the result of the query and store it in a variable
    table_3= cursor.fetchall()
    # Commit the transaction
    mydb.commit()
    # Create a pandas DataFrame from the fetched data
    df_3= pd.DataFrame(table_3, columns=("states", "transaction_count"))
    # Create a horizontal bar chart for average transaction count per state
    # Y-axis as states (horizontal bars), X-axis as the average transaction count, Title of the chart,Display state names on hover
    # Horizontal orientation of the bars, Custom color scheme,Height of the chart, Width of the chart
    fig_amount_3= px.bar(df_3, y="states", x="transaction_count", title="AVERAGE OF TRANSACTION COUNT", hover_name= "states", orientation= "h",
                        color_discrete_sequence=px.colors.sequential.Bluered_r, height= 800,width= 1000)
    # Render the chart in the Streamlit app
    st.plotly_chart(fig_amount_3)


#sql connection
def top_chart_registered_user(table_name, state):
    # Establishing a connection to the PostgreSQL database
    mydb= psycopg2.connect(host= "localhost",         # Hostname of the database server
                        user= "postgres",             # Database user
                        port= "5432",                 # Port on which PostgreSQL is running
                        database= "phonepe_data",     # Name of the database
                        password= "Nanu_300119")      # User password for accessing the database
    # Creating a cursor object to interact with the database
    cursor= mydb.cursor()

    #plot_1
    # SQL query to get the top 10 districts with the highest number of registered users in the given state
    query1= f'''SELECT districts, SUM(registereduser) AS registereduser
                FROM {table_name}
                WHERE states= '{state}'
                GROUP BY districts
                ORDER BY registereduser DESC
                LIMIT 10;'''
    # Executing the query
    cursor.execute(query1)
    # Fetching all the results of the query
    table_1= cursor.fetchall()
    # Committing the transaction to the database (optional for SELECT queries)
    mydb.commit()
    # Creating a pandas DataFrame from the query results
    df_1= pd.DataFrame(table_1, columns=("districts", "registereduser"))
    # Creating two columns in the Streamlit layout
    col1,col2= st.columns(2)
    # Plotting the first bar chart in the first column
    with col1:
        # Creating a bar chart using Plotly Express for the top 10 districts by registered users
        # X-axis: districts, Y-axis: sum of registered users, Title of the chart,Display district names on hover
        # Color scheme, Height of the chart,Width of the chart
        fig_amount= px.bar(df_1, x="districts", y="registereduser", title="TOP 10 OF REGISTERED USER", hover_name= "districts", 
                            color_discrete_sequence=px.colors.sequential.Aggrnyl, height= 650,width= 600)
        # Displaying the chart in the Streamlit app
        st.plotly_chart(fig_amount)

    #plot_2
    # SQL query to get the last 10 districts with the lowest number of registered users in the given state
    query2= f'''SELECT districts, SUM(registereduser) AS registereduser 
                FROM {table_name}
                WHERE states= '{state}'        # Filtering by the selected state
                GROUP BY districts             # Grouping by districts
                ORDER BY registereduser        # Sorting by total registered users in ascending order
                LIMIT 10;'''                   # Limiting the result to the bottom 10 districts
    # Executing the query
    cursor.execute(query2)
    # Fetching the query results
    table_2= cursor.fetchall()
    # Committing the transaction to the database
    mydb.commit()
    # Creating a pandas DataFrame from the fetched results
    df_2= pd.DataFrame(table_2, columns=("districts", "registereduser"))
    # Plotting the second bar chart in the second column
    with col2:
        # Creating a bar chart for the bottom 10 districts by registered users
        # Title of the chart, Display district names on hover, Reversed color scheme, Height of the chart, Width of the chart
        fig_amount_2= px.bar(df_2, x="districts", y="registereduser", title="LAST 10 REGISTERED USER", hover_name= "districts",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl_r, height= 650,width= 600)
        # Displaying the second chart in the Streamlit app
        st.plotly_chart(fig_amount_2)

    #plot_3
    # SQL query to get the average number of registered users for each district in the given state
    query3= f'''SELECT districts, AVG(registereduser) AS registereduser
                FROM {table_name}                   
                WHERE states= '{state}'         
                GROUP BY districts              
                ORDER BY registereduser;'''     
    # Executing the third query
    cursor.execute(query3)
    # Fetching the results of the query
    table_3= cursor.fetchall()
    # Committing the transaction
    mydb.commit()
    # Creating a pandas DataFrame from the query results
    df_3= pd.DataFrame(table_3, columns=("districts", "registereduser"))
    # Plotting the third chart - a horizontal bar chart for average registered users
    # Y-axis: districts, X-axis: average registered users,Title of the chart, Display district names on hover
    # Horizontal orientation, Custom color scheme, Height of the chart, Width of the chart
    fig_amount_3= px.bar(df_3, y="districts", x="registereduser", title="AVERAGE OF REGISTERED USER", hover_name= "districts", orientation= "h",
                        color_discrete_sequence=px.colors.sequential.Bluered_r, height= 800,width= 1000)
    # Displaying the third chart in the Streamlit app
    st.plotly_chart(fig_amount_3)

#sql connection
def top_chart_appopens(table_name, state):
    # Establishing a connection to the PostgreSQL database
    mydb= psycopg2.connect(host= "localhost",           # Hostname of the database server
                        user= "postgres",               # Database user
                        port= "5432",                   # Port on which PostgreSQL is running
                        database= "phonepe_data",       # Name of the database
                        password= "Nanu_300119")        # User password for accessing the database
    # Creating a cursor object to execute SQL queries
    cursor= mydb.cursor()

    #plot_1
    # SQL query to get the top 10 districts with the highest number of app opens in the specified state
    query1= f'''SELECT districts, SUM(appopens) AS appopens
                FROM {table_name}
                WHERE states= '{state}'
                GROUP BY districts
                ORDER BY appopens DESC
                LIMIT 10;'''
    # Executing the query
    cursor.execute(query1)
    # Fetching all the results of the query
    table_1= cursor.fetchall()
    # Committing the transaction to ensure any changes made by the query are saved (not needed for SELECT, but good practice)
    mydb.commit()
    # Creating a pandas DataFrame from the query results
    df_1= pd.DataFrame(table_1, columns=("districts", "appopens"))

    # Creating two columns in the Streamlit layout
    col1,col2= st.columns(2)
    # Plotting the first bar chart in the first column
    with col1:
        # Creating a bar chart using Plotly Express for the top 10 districts by app opens
        # X-axis: districts, Y-axis: total app opens, Title of the chart,Display district names on hover
        # Color scheme, Height of the chart, Width of the chart
        fig_amount= px.bar(df_1, x="districts", y="appopens", title="TOP 10 OF APPOPENS", hover_name= "districts",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl, height= 650,width= 600)
        # Displaying the chart in the Streamlit app
        st.plotly_chart(fig_amount)

    #plot_2 Last 10 App Opens per District
    # SQL query to select the districts and their total number of app opens in a specific state.
    # The results are grouped by districts and ordered by app opens in ascending order, limiting the results to 10.
    query2= f'''SELECT districts, SUM(appopens) AS appopens
                FROM {table_name}
                WHERE states= '{state}'
                GROUP BY districts
                ORDER BY appopens
                LIMIT 10;'''
    # Execute the query and fetch the results
    cursor.execute(query2)
    table_2= cursor.fetchall()
    # Commit the transaction (even though no changes are made to the database, this ensures safe completion of the query)
    mydb.commit()
    # Convert the query result into a Pandas DataFrame for easier manipulation and plotting.
    # The DataFrame contains two columns: 'districts' and 'appopens'.
    df_2= pd.DataFrame(table_2, columns=("districts", "appopens"))
    # Streamlit container (col2) to place the chart in a specific section of the layout
    with col2:
    # Create a bar plot using Plotly Express, displaying districts on the x-axis and total app opens on the y-axis.
    # 'Aggrnyl_r' color scheme is used to color the bars, which provides a green-to-blue gradient.
    # The chart is given a title, and districts are shown as a hover effect.
        fig_amount_2= px.bar(df_2, x="districts", y="appopens", title="LAST 10 APPOPENS", hover_name= "districts",  # Chart title  , Display district names when hovered
                            color_discrete_sequence=px.colors.sequential.Aggrnyl_r, height= 650,width= 600) # Color scheme for the bars,Height of the plot,Width of the plot
        # Display the bar chart in the Streamlit app
        st.plotly_chart(fig_amount_2)

    #plot_3 Average App Opens per District
    # SQL query to select the districts and their average number of app opens in the specific state.
    # The results are grouped by districts and ordered by the average app opens.
    query3= f'''SELECT districts, AVG(appopens) AS appopens
                FROM {table_name}
                WHERE states= '{state}'
                GROUP BY districts
                ORDER BY appopens;'''
    # Execute the query and fetch the results
    cursor.execute(query3)
    table_3= cursor.fetchall()
    # Commit the transaction to ensure it's safely completed
    mydb.commit()
    # Convert the query result into a Pandas DataFrame with two columns: 'districts' and 'appopens'.
    df_3= pd.DataFrame(table_3, columns=("districts", "appopens"))
    # Create a horizontal bar plot using Plotly Express, displaying districts on the y-axis and average app opens on the x-axis.
    # The chart uses a 'Bluered_r' color scheme, which is a red-to-blue gradient.
    # The chart is given a title, and the hover effect displays district names.
    # Chart title, Display district names when hovered, Set the orientation to horizontal
    # Color scheme for the bars, Height of the plot, Width of the plot
    fig_amount_3= px.bar(df_3, y="districts", x="appopens", title="AVERAGE OF APPOPENS", hover_name= "districts", orientation= "h",
                        color_discrete_sequence=px.colors.sequential.Bluered_r, height= 800,width= 1000)
    # Display the bar chart in the Streamlit app
    st.plotly_chart(fig_amount_3)

#sql connection
# Function to plot charts for registered users in different states
def top_chart_registered_users(table_name):
    # Establish connection to the PostgreSQL database
    mydb= psycopg2.connect(host= "localhost",           # Database server location
                        user= "postgres",               # Username for the database
                        port= "5432",                   # Port number for PostgreSQL
                        database= "phonepe_data",       # Name of the database      
                        password= "Nanu_300119")        # Password for the database
    # Create a cursor object to interact with the database
    cursor= mydb.cursor()

    #plot_1
    #Top 10 states with the highest number of registered users
    # SQL query to get the top 10 states with the highest number of registered users
    query1= f'''SELECT states, SUM(registeredusers) AS registeredusers
                FROM {table_name}
                GROUP BY states
                ORDER BY registeredusers DESC
                LIMIT 10;'''
    # Execute the SQL query and fetch all the results
    cursor.execute(query1)
    table_1= cursor.fetchall()
    # Commit the transaction to ensure everything is finalized safely
    mydb.commit()
    # Convert the result into a Pandas DataFrame with columns 'states' and 'registeredusers'
    df_1= pd.DataFrame(table_1, columns=("states", "registeredusers"))
    # Create two columns in Streamlit to place the charts side by side
    col1,col2= st.columns(2)
    # Plot the first chart in col1
    with col1:
        # Create a bar chart using Plotly Express to show the top 10 states with the most registered users
        # Chart title, Show state names when hovered over the bars, Use 'Aggrnyl' color sequence for the bars
        # Set the height of the plot, Set the width of the plot
        fig_amount= px.bar(df_1, x="states", y="registeredusers", title="TOP 10 OF REGISTERED USERS", hover_name= "states",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl, height= 650,width= 600)
        # Render the bar chart in the Streamlit app
        st.plotly_chart(fig_amount)

    #plot_2 Last 10 states with the lowest number of registered users
    # SQL query to get the states with the least registered users (ascending order)
    query2= f'''SELECT states, SUM(registeredusers) AS registeredusers
                FROM {table_name}
                GROUP BY states
                ORDER BY registeredusers
                LIMIT 10;'''
    # Execute the query and fetch all the results
    cursor.execute(query2)
    table_2= cursor.fetchall()
    # Commit the transaction
    mydb.commit()
    # Convert the result into a Pandas DataFrame with columns 'states' and 'registeredusers'
    df_2= pd.DataFrame(table_2, columns=("states", "registeredusers"))

    # Plot the second chart in col2
    with col2:
        # Create a bar chart using Plotly Express to show the last 10 states with the lowest registered users
        # Chart title, Show state names when hovered over the bars,Use 'Aggrnyl_r' color sequence for the bars
        # Set the height of the plot, Set the width of the plot
        fig_amount_2= px.bar(df_2, x="states", y="registeredusers", title="LAST 10 REGISTERED USERS", hover_name= "states",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl_r, height= 650,width= 600)
        # Render the bar chart in the Streamlit app
        st.plotly_chart(fig_amount_2)

    #plot_3 #Average number of registered users per state
    # SQL query to get the average number of registered users per state
    query3= f'''SELECT states, AVG(registeredusers) AS registeredusers
                FROM {table_name}
                GROUP BY states
                ORDER BY registeredusers;'''
    # Execute the query and fetch all the results
    cursor.execute(query3)
    table_3= cursor.fetchall()
    # Commit the transaction
    mydb.commit()
    # Convert the result into a Pandas DataFrame with columns 'states' and 'registeredusers'
    df_3= pd.DataFrame(table_3, columns=("states", "registeredusers"))
    # Create a horizontal bar chart using Plotly Express to show the average number of registered users per state
    # Chart title, Show state names when hovered over the bars
    # Set the orientation to horizontal, Use 'Bluered_r' color sequence
    # Set the height of the plot, Set the width of the plot
    fig_amount_3= px.bar(df_3, y="states", x="registeredusers", title="AVERAGE OF REGISTERED USERS", hover_name= "states", orientation= "h",
                        color_discrete_sequence=px.colors.sequential.Bluered_r, height= 800,width= 1000)
    # Render the bar chart in the Streamlit app
    st.plotly_chart(fig_amount_3)


# Streamlit Part
# Set the layout of the Streamlit app to be "wide" (spanning the full width of the screen)
st.set_page_config(layout= "wide")
# Set the title of the app
st.title("PHONEPE DATA VISUALIZATION AND EXPLORATION")
# Sidebar menu for navigation
with st.sidebar:
    # Add an option menu in the sidebar to select between different pages  
    select= option_menu("Main Menu",["HOME", "DATA EXPLORATION", "TOP CHARTS"])
# Conditional display for the "HOME" section
if select == "HOME":
    # Create two columns (col1 and col2) for a split layout
    col1,col2= st.columns(2)
    # Left column (col1)
    with col1:
        # Add headers, subheaders, markdown, and write functions for textual information
        st.header("PHONEPE")
        st.subheader("INDIA'S BEST TRANSACTION APP")
        st.markdown("PhonePe  is an Indian digital payments and financial technology company")
        # Displaying various features of PhonePe using `st.write`
        st.write("****FEATURES****")
        st.write("****Credit & Debit card linking****")
        st.write("****Bank Balance check****")
        st.write("****Money Storage****")
        st.write("****PIN Authorization****")
        # Add a download button with a link to download the PhonePe app
        st.download_button("DOWNLOAD THE APP NOW", "https://www.phonepe.com/app-download/")
    # Right column (col2)
    with col2:
        # Display an image in col2 using the file path
        st.image(Image.open(r"C:\Users\bhava\Desktop\Data_Science_IITM\My_CapStoneProject1\phonepay\phonepayimage2.jpg"),width= 500)
    # Create another set of columns (col3 and col4)
    col3,col4= st.columns(2)
    # Left column (col3) to display an image
    with col3:
        st.image(Image.open(r"C:\Users\bhava\Desktop\Data_Science_IITM\My_CapStoneProject1\phonepay\phonepayimage2.jpg"),width=400)
    # Right column (col4) for additional text content
    with col4:
        st.write("****Easy Transactions****")
        st.write("****One App For All Your Payments****")
        st.write("****Your Bank Account Is All You Need****")
        st.write("****Multiple Payment Modes****")
        st.write("****PhonePe Merchants****")
        st.write("****Multiple Ways To Pay****")
        st.write("****1.Direct Transfer & More****")
        st.write("****2.QR Code****")
        st.write("****Earn Great Rewards****")
    # Create a final set of columns (col5 and col6)
    col5,col6= st.columns(2)
    # Left column (col5) for more text
    with col5:
        # Use markdown to add some vertical spacing and then add more features of the app
        st.markdown(" ")    # Adding space between sections
        st.markdown(" ")    # Repeat this for additional spacing
        st.markdown(" ")
        st.markdown(" ")
        st.markdown(" ")
        st.markdown(" ")
        st.markdown(" ")
        st.markdown(" ")
        st.markdown(" ")
        st.write("****No Wallet Top-Up Required****")
        st.write("****Pay Directly From Any Bank To Any Bank A/C****")
        st.write("****Instantly & Free****")
    # Right column (col6) to display the last image
    with col6:
        st.image(Image.open(r"C:\Users\bhava\Desktop\Data_Science_IITM\My_CapStoneProject1\phonepay\phonepayimage2.jpg"),width= 500)

# Check if the user has selected "DATA EXPLORATION" from the sidebar
elif select == "DATA EXPLORATION":
    # Create three tabs for different types of analysis
    tab1, tab2, tab3 = st.tabs(["Aggregated Analysis", "Map Analysis", "Top Analysis"])
    # Aggregated Analysis Tab
    with tab1:
        # Radio button to select the type of analysis: Insurance, Transaction, or User
        method = st.radio("Select The Method",["Insurance Analysis", "Transaction Analysis", "User Analysis"])
        # Insurance Analysis section
        if method == "Insurance Analysis":
            # Create two columns to divide the layout
            col1,col2= st.columns(2)
            with col1:
                # Slider to select the year for insurance data
                years= st.slider("Select The Year",Aggre_insurance["Years"].min(), Aggre_insurance["Years"].max(),Aggre_insurance["Years"].min())
            # Fetch insurance transaction amount and count based on selected year
            tac_Y= Transaction_amount_count_Y(Aggre_insurance, years)

            col1,col2= st.columns(2)
            with col1:
                # Slider to select the quarter
                quarters= st.slider("Select The Quarter",tac_Y["Quarter"].min(), tac_Y["Quarter"].max(),tac_Y["Quarter"].min())
            # Display insurance analysis for selected year and quarter
            Transaction_amount_count_Y_Q(tac_Y, quarters)
        # Transaction Analysis section
        elif method == "Transaction Analysis":
            
            col1,col2= st.columns(2)
            with col1:
                # Slider to select the year for transaction data
                years= st.slider("Select The Year",Aggre_transaction["Years"].min(), Aggre_transaction["Years"].max(),Aggre_transaction["Years"].min())
            # Fetch transaction data (amount and count) based on selected year
            Aggre_tran_tac_Y= Transaction_amount_count_Y(Aggre_transaction, years)

            col1,col2= st.columns(2)
            with col1:
                # Dropdown to select the state
                states= st.selectbox("Select The State", Aggre_tran_tac_Y["States"].unique())
            # Display transaction analysis based on selected state
            Aggre_Tran_Transaction_type(Aggre_tran_tac_Y, states)

            col1,col2= st.columns(2)
            with col1:
                # Slider to select the quarter for transaction data
                quarters= st.slider("Select The Quarter",Aggre_tran_tac_Y["Quarter"].min(), Aggre_tran_tac_Y["Quarter"].max(),Aggre_tran_tac_Y["Quarter"].min())
            # Fetch transaction data for selected year and quarter
            Aggre_tran_tac_Y_Q= Transaction_amount_count_Y_Q(Aggre_tran_tac_Y, quarters)

            col1,col2= st.columns(2)
            with col1:
                # Dropdown to select the state for further analysis based on quarter
                states= st.selectbox("Select The State_Ty", Aggre_tran_tac_Y_Q["States"].unique())
            # Display transaction analysis based on state and quarter
            Aggre_Tran_Transaction_type(Aggre_tran_tac_Y_Q, states)
        # User Analysis section
        elif method == "User Analysis":
            
            col1,col2= st.columns(2)
            with col1:
                # Slider to select the year for user data
                years= st.slider("Select The Year",Aggre_user["Years"].min(), Aggre_user["Years"].max(),Aggre_user["Years"].min())
            # Fetch user analysis data based on the selected year
            Aggre_user_Y= Aggre_user_plot_1(Aggre_user, years)

            col1,col2= st.columns(2)
            with col1:
                # Slider to select the quarter for user data
                quarters= st.slider("Select The Quarter",Aggre_user_Y["Quarter"].min(), Aggre_user_Y["Quarter"].max(),Aggre_user_Y["Quarter"].min())
            # Fetch user analysis data for the selected year and quarter
            Aggre_user_Y_Q= Aggre_user_plot_2(Aggre_user_Y, quarters)

            col1,col2= st.columns(2)
            with col1:
                # Dropdown to select the state for user analysis
                states= st.selectbox("Select The State", Aggre_user_Y_Q["States"].unique())
            # Display user analysis based on state, year, and quarter
            Aggre_user_plot_3(Aggre_user_Y_Q, states)



# Map Analysis Tab
    with tab2:
        # Radio button to select the method of analysis for the map: Insurance, Transaction, or User
        method_2= st.radio("Select The Method",["Map Insurance", "Map Transaction", "Map User"])
        # Map Insurance Analysis
        if method_2 == "Map Insurance":
            # Layout the page in two columns   
            col1,col2= st.columns(2)
            with col1:
                # Slider to select the year for the insurance map data
                years= st.slider("Select The Year_mi",map_insurance["Years"].min(), map_insurance["Years"].max(),map_insurance["Years"].min())
            # Get insurance data for the selected year
            map_insur_tac_Y= Transaction_amount_count_Y(map_insurance, years)

            col1,col2= st.columns(2)
            with col1:
                # Dropdown to select the state for insurance data
                states= st.selectbox("Select The State_mi", map_insur_tac_Y["States"].unique())
            # Plot insurance data at the district level for the selected state
            Map_insur_District(map_insur_tac_Y, states)

            col1,col2= st.columns(2)
            
            with col1:
                # Slider to select the quarter for the insurance data
                quarters= st.slider("Select The Quarter_mi",map_insur_tac_Y["Quarter"].min(), map_insur_tac_Y["Quarter"].max(),map_insur_tac_Y["Quarter"].min())
            map_insur_tac_Y_Q= Transaction_amount_count_Y_Q(map_insur_tac_Y, quarters)

            col1,col2= st.columns(2)
            with col1:
                # Dropdown to select the state for detailed analysis based on the selected year and quarter
                states= st.selectbox("Select The State_Ty", map_insur_tac_Y_Q["States"].unique())
            # Plot district-level insurance data for the selected state and quarter
            Map_insur_District(map_insur_tac_Y_Q, states)
        # Map Transaction Analysis
        elif method_2 == "Map Transaction":
            
            col1,col2= st.columns(2)
            with col1:
                # Slider to select the year for the transaction map data
                years= st.slider("Select The Year",map_transaction["Years"].min(), map_transaction["Years"].max(),map_transaction["Years"].min())
            # Get transaction data for the selected year
            map_tran_tac_Y= Transaction_amount_count_Y(map_transaction, years)

            col1,col2= st.columns(2)
            with col1:
                # Dropdown to select the state for transaction data
                states= st.selectbox("Select The State_mi", map_tran_tac_Y["States"].unique())
            # Plot transaction data at the district level for the selected state
            Map_insur_District(map_tran_tac_Y, states)

            col1,col2= st.columns(2)
            with col1:
                # Slider to select the quarter for the transaction data
                quarters= st.slider("Select The Quarter_mt",map_tran_tac_Y["Quarter"].min(), map_tran_tac_Y["Quarter"].max(),map_tran_tac_Y["Quarter"].min())
            # Get transaction data for the selected year and quarter
            map_tran_tac_Y_Q= Transaction_amount_count_Y_Q(map_tran_tac_Y, quarters)

            col1,col2= st.columns(2)
            with col1:
                # Dropdown to select the state for detailed analysis based on the selected year and quarter
                states= st.selectbox("Select The State_Ty", map_tran_tac_Y_Q["States"].unique())
            # Plot district-level transaction data for the selected state and quarter
            Map_insur_District(map_tran_tac_Y_Q, states)

        # Map User Analysis
        elif method_2 == "Map User":
            
            col1,col2= st.columns(2)
            with col1:
                # Slider to select the year for the user map data
                years= st.slider("Select The Year_mu",map_user["Years"].min(), map_user["Years"].max(),map_user["Years"].min())
            # Get user data for the selected year
            map_user_Y= map_user_plot_1(map_user, years)

            col1,col2= st.columns(2)
            with col1:
                # Slider to select the quarter for the user data
                quarters= st.slider("Select The Quarter_mu",map_user_Y["Quarter"].min(), map_user_Y["Quarter"].max(),map_user_Y["Quarter"].min())
            # Get user data for the selected year and quarter
            map_user_Y_Q= map_user_plot_2(map_user_Y, quarters)

            col1,col2= st.columns(2)
            with col1:
                # Dropdown to select the state for detailed user analysis
                states= st.selectbox("Select The State_mu", map_user_Y_Q["States"].unique())
            # Plot district-level user data for the selected state and quarter
            map_user_plot_3(map_user_Y_Q, states)

    # Top Analysis Tab
    with tab3:
        # Radio button to select the method of analysis for top data: Insurance, Transaction, or User
        method_3= st.radio("Select The Method",["Top Insurance", "Top Transaction", "Top User"])
        # Top Insurance Analysis
        if method_3 == "Top Insurance":
            # Layout the page in two columns
            col1,col2= st.columns(2)
            with col1:
                # Slider to select the year for top insurance data
                years= st.slider("Select The Year_ti",top_insurance["Years"].min(), top_insurance["Years"].max(),top_insurance["Years"].min())
            # Get insurance data for the selected year
            top_insur_tac_Y= Transaction_amount_count_Y(top_insurance, years)

            col1,col2= st.columns(2)
            with col1:
                # Dropdown to select the state for top insurance data
                states= st.selectbox("Select The State_ti", top_insur_tac_Y["States"].unique())
            # Plot top insurance data for the selected state
            Top_insurance_plot_1(top_insur_tac_Y, states)

            col1,col2= st.columns(2)
            with col1:
                # Slider to select the quarter for top insurance data
                quarters= st.slider("Select The Quarter_mu",top_insur_tac_Y["Quarter"].min(), top_insur_tac_Y["Quarter"].max(),top_insur_tac_Y["Quarter"].min())
            # Get insurance data for the selected year and quarter
            top_insur_tac_Y_Q= Transaction_amount_count_Y_Q(top_insur_tac_Y, quarters)

            
        # Top Transaction Analysis
        elif method_3 == "Top Transaction":
            
            col1,col2= st.columns(2)
            with col1:
                # Slider to select the year for top transaction data
                years= st.slider("Select The Year_tt",top_transaction["Years"].min(), top_transaction["Years"].max(),top_transaction["Years"].min())
            # Get transaction data for the selected year
            top_tran_tac_Y= Transaction_amount_count_Y(top_transaction, years)

            col1,col2= st.columns(2)
            with col1:
                # Dropdown to select the state for top transaction data
                states= st.selectbox("Select The State_tt", top_tran_tac_Y["States"].unique())
            # Plot top transaction data for the selected state
            Top_insurance_plot_1(top_tran_tac_Y, states)

            col1,col2= st.columns(2)
            with col1:
                # Slider to select the quarter for top transaction data
                quarters= st.slider("Select The Quarter_tt",top_tran_tac_Y["Quarter"].min(), top_tran_tac_Y["Quarter"].max(),top_tran_tac_Y["Quarter"].min())
            # Get transaction data for the selected year and quarter
            top_tran_tac_Y_Q= Transaction_amount_count_Y_Q(top_tran_tac_Y, quarters)

        # Top User Analysis
        elif method_3 == "Top User":
            
            col1,col2= st.columns(2)
            with col1:
                # Slider to select the year for top user data
                years= st.slider("Select The Year_tu",top_user["Years"].min(), top_user["Years"].max(),top_user["Years"].min())
            # Get user data for the selected year    
            top_user_Y= top_user_plot_1(top_user, years)

            col1,col2= st.columns(2)
            with col1:
                # Dropdown to select the state for top user data
                states= st.selectbox("Select The State_tu", top_user_Y["States"].unique())
            # Plot top user data for the selected state
            top_user_plot_2(top_user_Y, states)

# Check if the selected option is "TOP CHARTS"
elif select == "TOP CHARTS":
    # Display a dropdown (selectbox) in the Streamlit app to allow users to select the question
    # It provides different options related to Transaction Amount, Count, Registered users, etc.
    question= st.selectbox("Select the Question",["1. Transaction Amount and Count of Aggregated Insurance",
                                                    "2. Transaction Amount and Count of Map Insurance",
                                                    "3. Transaction Amount and Count of Top Insurance",
                                                    "4. Transaction Amount and Count of Aggregated Transaction",
                                                    "5. Transaction Amount and Count of Map Transaction",
                                                    "6. Transaction Amount and Count of Top Transaction",
                                                    "7. Transaction Count of Aggregated User",
                                                    "8. Registered users of Map User",
                                                    "9. App opens of Map User",
                                                    "10. Registered users of Top User",
                                                    ])
    
    # Case 1: If the first option is selected (Transaction Amount and Count of Aggregated Insurance)
    if question == "1. Transaction Amount and Count of Aggregated Insurance":
        # Display a header for the transaction amount section
        st.subheader("TRANSACTION AMOUNT")
        # Call the function to display the top charts for transaction amount, passing "aggregated_insurance" as the category
        top_chart_transaction_amount("aggregated_insurance")
        # Display a header for the transaction count section
        st.subheader("TRANSACTION COUNT")
        # Call the function to display the top charts for transaction count, passing "aggregated_insurance" as the category
        top_chart_transaction_count("aggregated_insurance") 

    # Case 2: If the second option is selected (Transaction Amount and Count of Map Insurance)
    elif question == "2. Transaction Amount and Count of Map Insurance":
        # Display a header for the transaction amount section
        st.subheader("TRANSACTION AMOUNT")
        # Call the function to display the top charts for transaction amount, passing "map_insurance" as the category
        top_chart_transaction_amount("map_insurance")
        # Display a header for the transaction count section
        st.subheader("TRANSACTION COUNT")
        # Call the function to display the top charts for transaction count, passing "map_insurance" as the category
        top_chart_transaction_count("map_insurance")

    # Case 3: If the third option is selected (Transaction Amount and Count of Top Insurance)
    elif question == "3. Transaction Amount and Count of Top Insurance":
        # Display a header for the transaction amount section
        st.subheader("TRANSACTION AMOUNT")
        # Call the function to display the top charts for transaction amount, passing "top_insurance" as the category
        top_chart_transaction_amount("top_insurance")
        # Display a header for the transaction count section
        st.subheader("TRANSACTION COUNT")
        # Call the function to display the top charts for transaction count, passing "top_insurance" as the category
        top_chart_transaction_count("top_insurance")

    # Case 4: If the fourth option is selected (Transaction Amount and Count of Aggregated Transaction)
    elif question == "4. Transaction Amount and Count of Aggregated Transaction":
        # Display a header for the transaction amount section
        st.subheader("TRANSACTION AMOUNT")
        # Call the function to display the top charts for transaction amount, passing "aggregated_transaction" as the category
        top_chart_transaction_amount("aggregated_transaction")
        # Display a header for the transaction count section
        st.subheader("TRANSACTION COUNT")
        # Call the function to display the top charts for transaction count, passing "aggregated_transaction" as the category
        top_chart_transaction_count("aggregated_transaction")

    # Case 5: If the fifth option is selected (Transaction Amount and Count of Map Transaction)
    elif question == "5. Transaction Amount and Count of Map Transaction":
        # Display a header for the transaction amount section
        st.subheader("TRANSACTION AMOUNT")
        # Call the function to display the top charts for transaction amount, passing "map_transaction" as the category
        top_chart_transaction_amount("map_transaction")
        # Display a header for the transaction count section
        st.subheader("TRANSACTION COUNT")
        # Call the function to display the top charts for transaction count, passing "map_transaction" as the category
        top_chart_transaction_count("map_transaction")

    # Case 6: If the sixth option is selected (Transaction Amount and Count of Top Transaction)
    elif question == "6. Transaction Amount and Count of Top Transaction":
        # Display a header for the transaction amount section
        st.subheader("TRANSACTION AMOUNT")
        # Call the function to display the top charts for transaction amount, passing "top_transaction" as the category
        top_chart_transaction_amount("top_transaction")
        # Display a header for the transaction count section
        st.subheader("TRANSACTION COUNT")
        # Call the function to display the top charts for transaction count, passing "top_transaction" as the category
        top_chart_transaction_count("top_transaction")

    # Case 7: If the seventh option is selected (Transaction Count of Aggregated User)
    elif question == "7. Transaction Count of Aggregated User":
        # Display a header for the transaction count section
        st.subheader("TRANSACTION COUNT")
        # Call the function to display the top charts for transaction count, passing "aggregated_user" as the category
        top_chart_transaction_count("aggregated_user")

    # Case 8: If the eighth option is selected (Registered users of Map User)
    elif question == "8. Registered users of Map User":
        # Provide a dropdown for the user to select a state
        states= st.selectbox("Select the State", map_user["States"].unique())   
        # Display a header for the registered users section
        st.subheader("REGISTERED USERS")
        # Call the function to display the top charts for registered users of a selected state in the "map_user" category
        top_chart_registered_user("map_user", states)

    # Case 9: If the ninth option is selected (App opens of Map User)
    elif question == "9. App opens of Map User":
        # Provide a dropdown for the user to select a state
        states= st.selectbox("Select the State", map_user["States"].unique())   
        # Display a header for the app opens section
        st.subheader("APPOPENS")
        # Call the function to display the top charts for app opens for a selected state in the "map_user" category
        top_chart_appopens("map_user", states)

    # Case 10: If the tenth option is selected (Registered users of Top User)
    elif question == "10. Registered users of Top User":
        # Display a header for the registered users section  
        st.subheader("REGISTERED USERS")
        # Call the function to display the top charts for registered users in the "top_user" category
        top_chart_registered_users("top_user")