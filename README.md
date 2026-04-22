# PyPublica
PyPublica is an open-source tool that helps people find open-access scientific articles using filters. PyPublica supports searching for over 1000 papers per request and includes a batch download feature. PyPublica is built with Python, using Tkinter for the GUI.
<br>

## How It Works
PyPublica utilizes the public search query APIs provided by Elsevier and NCBI. To ensure a seamless downloading experience, the tool also incorporates Unpaywall API calls. Consequently, users need an Elsevier Developer account and API key, as well as an NCBI account and API key. To help you get started, the creator has provided a tutorial below.

## Disclaimer
The creator acknowledges using an AI code agent to assist in building the GUI. The original workflow was designed entirely for a terminal-based command-line interface (CLI). However, to make the tool more accessible to users unfamiliar with the Python Terminal interpreter, the creator chose to incorporate an AI code agent to develop a more user-friendly interface based on Tkinter. The creator fully accepts any criticism regarding the use of AI in this project. If you prefer not to use AI-generated code, please use the following Python script instead:

>[pypublica_python.py](Naminano/PyPublica/src/pypublica_python.py)


## Tutorial - GUI-Based App
### 1. Install The Setup
On Windows, you can simply use the setup file and follow the installation process:
>[Setup.exe](https://github.com/adhiar/PyPublica/releases/tag/release) <br>

Alternatively, you can extract the ZIP file here:
>[PyPublica.zip](https://github.com/adhiar/PyPublica/releases/tag/release) <br>

### 2. Open The App 
The first time you open the app, a credentials window will appear. You will need to provide your Elsevier API Key, NCBI Email, and NCBI API Key. These credentials only need to be entered once, as they will be saved securely on your local machine. You will not need to enter them again the next time you open the app.

> _This workflow uses the Keyring library to store your credentials securely on your local system. Your information is not linked to any online server or website, ensuring a basic level of security._

It's recommended to watch the tutorial video if this is your first time using an Elsevier API Key, NCBI Email, or NCBI API Key for querying.

### 3. Enjoy Searching
After completed all the steps, now you can start searching. To ensure theres no copyright issues, only **open acces journal** would be appear in the search result.

