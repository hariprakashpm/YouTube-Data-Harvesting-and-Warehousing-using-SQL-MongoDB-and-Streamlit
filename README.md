# YouTube-Data-Harvesting-and-Warehousing-using-SQL-MongoDB-and-Streamlit

Project Title: YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit

Skills take away From This Project: Python scripting, Data Collection,MongoDB, Streamlit, API integration, Data Managment using MongoDB (Atlas) and SQL.

Domain : Social Media (YouTube)

Problem Statement:

The problem statement is to create a Streamlit application that allows users to access and analyze data from multiple YouTube channels. 

About the Project:

1.	The provided code is a Python script that uses the Streamlit library to create a web application for fetching data from the YouTube API,storing it in a MongoDB database, and uploading it to a MySQL database for further analysis. Here's a brief explanation of the code:

2.	The script imports necessary libraries and modules, including Streamlit, Google API client, pymongo, datetime, time, pandas, numpy, matplotlib and mysql.connector.

3.	It sets the Streamlit page configuration and displays a title for the web application.

4.	The code defines several functions for fetching data from the YouTube API, including channel statistics, playlist data, video IDs, video details and comment details. These functions use the provided YouTube API key to make API requests and retrieve the desired data.

5.	The code defines a function to fetch channel names from the MongoDB database.

6.	The script creates a connection to the MongoDB database and defines collections for channel data, video data, and comment data.

7.	The code checks for user input (channel ID) and a button click to fetch channel details and upload them to the MongoDB database.It calls the previously defined functions to fetch channel details, playlist data, video IDs, video details, and comment details.The fetched data is then inserted into the corresponding collections in the MongoDB database.

8.	After fetching and uploading the data to MongoDB, the script provides an option for the user to select channels for uploading the data to a MySQL database. It displays a multiselect input for channel selection and a button to upload the data to MySQL.

9.	When the button to upload data to MySQL is clicked, the script fetches channel details, video details, and comment details from the MongoDB collections. It establishes a connection to the MySQL database and inserts the data into corresponding tables using SQL queries. It uses pandas and the SQLAlchemy engine for the data insertion process.

10.	Finally, the script provides a selection box for the user to choose from several predefined questions. Based on the selected question, the script executes
11.	corresponding SQL queries on the MySQL database and retrieves the results. The results are displayed in a table or a bar chart using Streamlit's table and bar_chart functions.

12.	Overall, the code combines the functionalities of fetching data from the YouTube API, storing it in MongoDB, uploading it to MySQL, and displaying the results in a web application using Streamlit.



Acknowledgment:

If you have any questions, suggestions, or feedback, please feel free to contact me at hariprakashpm@gmail.com

Thank You...!!!
