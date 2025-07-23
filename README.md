# Getting Started with scrape_dagster

# Clone the Repository

```bash
git clone https://github.com/tsss-jurylaw/Jury_dagster.git
```

```bash
cd Jury_dagster
```

# Create a Virtual Environment

# For Windows:

- Run the following command to create a virtual environment:

```bash
python -m venv venv
```
- Activate the virtual environment:

```bash
venv\Scripts\activate
```

# For Linux/MacOS:

- Run the following command to create a virtual environment:

```bash
python3 -m venv venv
```
- Activate the virtual environment:

```bash
source venv/bin/activate
```

# Install Dependencies

Once the virtual environment is activated, install the required dependencies:

```bash
pip install -r requirements.txt
```

Add Environment Variables

Create a .env file in the project root directory to store environment-specific variables.
Example .env file:

# Add your environment variables here

copy .env.sample to .env 

# Run the Project

Start the Dagster development server by running:

```bash
dagster dev
```

- Open http://localhost:4005 with your browser to see the project.

