import re
import streamlit as st
import pandas as pd
import psycopg2
from dotenv import load_dotenv
# from openai import OpenAI
import google.generativeai as genai
import os
import bcrypt
import time


load_dotenv()  # reads variables from a .env file and sets them in os.environ

GEMINI_API_KEY = st.secrets["GEMINI_API_KEY"]
HASHED_PASSWORD = st.secrets["HASHED_PASSWORD"].encode("utf-8")


# Database schema for context
DATABASE_SCHEMA = """
Database Schema (Anime Data Warehouse):

LOOKUP / DIMENSION TABLES:
- anime_types (
    type_id   SERIAL PRIMARY KEY,
    type_name TEXT NOT NULL UNIQUE
  )

- anime_statuses (
    status_id   SERIAL PRIMARY KEY,
    status_desc TEXT NOT NULL UNIQUE
  )

- studios (
    studio_id   SERIAL PRIMARY KEY,
    studio_name TEXT NOT NULL UNIQUE
  )

- sources (
    source_id   SERIAL PRIMARY KEY,
    source_name TEXT NOT NULL UNIQUE
  )

- rating_categories (
    rating_category_id SERIAL PRIMARY KEY,
    rating_code        TEXT NOT NULL UNIQUE
  )

- genres (
    genre_id   SERIAL PRIMARY KEY,
    genre_name TEXT NOT NULL UNIQUE
  )

- countries (
    country_id   SERIAL PRIMARY KEY,
    country_name TEXT NOT NULL UNIQUE
  )

- age_groups (
    age_group_id    SERIAL PRIMARY KEY,
    age_group_label TEXT NOT NULL UNIQUE
  )

- genders (
    gender_id   SERIAL PRIMARY KEY,
    gender_desc TEXT NOT NULL UNIQUE
  )

- watch_statuses (
    watch_status_id SERIAL PRIMARY KEY,
    status_desc     TEXT NOT NULL UNIQUE
  )

CORE / ENTITY TABLES:
- anime (
    anime_id           TEXT PRIMARY KEY,
    title              TEXT NOT NULL,
    type_id            INTEGER REFERENCES anime_types(type_id),
    status_id          INTEGER REFERENCES anime_statuses(status_id),
    episodes           INTEGER,
    start_date         TIMESTAMP,
    end_date           TIMESTAMP,
    source_id          INTEGER REFERENCES sources(source_id),
    studio_id          INTEGER REFERENCES studios(studio_id),
    rating_category_id INTEGER REFERENCES rating_categories(rating_category_id),
    overall_score      REAL,
    popularity_rank    INTEGER
  )

- users (
    user_id      TEXT PRIMARY KEY,
    user_name    TEXT NOT NULL,
    country_id   INTEGER REFERENCES countries(country_id),
    age_group_id INTEGER REFERENCES age_groups(age_group_id),
    gender_id    INTEGER REFERENCES genders(gender_id)
  )

FACT TABLES:
- anime_genres (
    anime_id TEXT NOT NULL REFERENCES anime(anime_id),
    genre_id INTEGER NOT NULL REFERENCES genres(genre_id),
    PRIMARY KEY (anime_id, genre_id)
  )

- user_anime_ratings (
    user_id        TEXT NOT NULL REFERENCES users(user_id),
    anime_id       TEXT NOT NULL REFERENCES anime(anime_id),
    user_score     REAL,
    rating_date    TIMESTAMP,
    watch_status_id INTEGER REFERENCES watch_statuses(watch_status_id),
    PRIMARY KEY (user_id, anime_id)
  )

IMPORTANT NOTES:
- Use JOINs to bring in descriptive values from lookup tables (types, genres, studios, countries, etc.)
- Typical joins:
    anime  ↔ anime_types, anime_statuses, studios, sources, rating_categories
    anime  ↔ anime_genres ↔ genres
    users  ↔ countries, age_groups, genders
    user_anime_ratings ↔ users, anime, watch_statuses
- overall_score and user_score are REAL (numeric) values
- rating_date, start_date, and end_date are TIMESTAMP types
- You can compute aggregates like AVG(user_score), COUNT(*), etc.
- For popularity_rank: lower value = more popular
"""



def login_screen():
    """Display login screen and authenticate user."""
    st.title("🔐 Secure Login")
    st.markdown("---")
    st.write("Enter your password to access the AI SQL Query Assistant.")
    
    password = st.text_input("Password", type="password", key="login_password")
    
    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        login_btn = st.button("🔓 Login", type="primary", use_container_width=True)
    
    if login_btn:
        if password:
            try:
                if bcrypt.checkpw(password.encode('utf-8'), HASHED_PASSWORD):
                    st.session_state.logged_in = True
                    st.success("✅ Authentication successful! Redirecting...")
                    st.rerun()
                else:
                    st.error("❌ Incorrect password")
            except Exception as e:
                st.error(f"❌ Authentication error: {e}")
        else:
            st.warning("⚠️ Please enter a password")
    
    st.markdown("---")
    st.info("""
    **Security Notice:**
    - Passwords are protected using bcrypt hashing
    - Your session is secure and isolated
    - You will remain logged in until you close the browser or click logout
    """)


def require_login():
    """Enforce login before showing main app."""
    if "logged_in" not in st.session_state or not st.session_state.logged_in:
        login_screen()
        st.stop()

@st.cache_resource
def get_db_url():
    POSTGRES_USERNAME = st.secrets["POSTGRES_USERNAME"]
    POSTGRES_PASSWORD = st.secrets["POSTGRES_PASSWORD"]
    POSTGRES_SERVER = st.secrets["POSTGRES_SERVER"]
    POSTGRES_DATABASE = st.secrets["POSTGRES_DATABASE"]

    DATABASE_URL = f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_SERVER}/{POSTGRES_DATABASE}"

    return DATABASE_URL

DATABASE_URL = get_db_url()


# @st.cache_resource
# def get_db_connection():

#     """Create and cache database connection."""
#     try:
#         conn = psycopg2.connect(DATABASE_URL)
#         return conn
#     except Exception as e:
#         st.error(f"Failed to connect to database: {e}")
#         return None
    
def get_db_connection():
    if 'db_conn' in st.session_state:
        try:
            st.session_state.db_conn.cursor().execute("SELECT 1;")
            return st.session_state.db_conn
        except:
            st.session_state.db_conn = None

    # connect fresh
    try:
        conn = psycopg2.connect(DATABASE_URL)
        st.session_state.db_conn = conn
        return conn
    except Exception as e:
        st.error(f"Failed to connect to database: {e}")
        return None

def run_query(sql):
    """Execute SQL query and return results as DataFrame."""
    conn = get_db_connection()
    if conn is None:
        return None
    
    try:
        df = pd.read_sql_query(sql, conn)
        return df
    except Exception as e:
        st.error(f"Error executing query: {e}")
        return None 
    

# @st.cache_resource
# def get_openai_client():
#     """Create and cache OpenAI client."""
#     return OpenAI(api_key=OPENAI_API_KEY)

@st.cache_resource
def get_gemini_client():
    """Create and cache Gemini client."""
    genai.configure(api_key=GEMINI_API_KEY)
    return genai.GenerativeModel("models/gemini-2.5-flash")


def extract_sql_from_response(response_text):
    clean_sql = re.sub(r"^```sql\s*|\s*```$", "", response_text, flags=re.IGNORECASE | re.MULTILINE).strip()
    return clean_sql


def generate_sql_with_gpt(user_question):
    client = get_gemini_client()
    prompt = f"""You are a STRICT PostgreSQL expert and an assistant for an Anime analytics database. Given the following database schema and a user's question, generate a valid, accurate PostgreSQL query.

    {DATABASE_SCHEMA}

    User Question: {user_question}

    Requirements:
    1. Generate ONLY the SQL query that I can directly use. No explanation, no backticks.
    2. Use proper JOINs to bring in descriptive names from lookup tables
    3. Use appropriate aggregations (COUNT, AVG, SUM, etc.) when needed.
    4. Add LIMIT clauses for queries that might return many rows (default LIMIT 100).
    5. Use proper date/time functions for TIMESTAMP columns (e.g., rating_date, start_date).
    6. Make sure the query is syntactically correct for PostgreSQL.
    7. Add helpful column aliases using AS.

    Generate the SQL query:"""

    try:
        response = client.generate_content(prompt)
        sql_query = extract_sql_from_response(response.text)
        return sql_query

    except Exception as e:
        st.error(f"Error calling Gemini API: {e}")
        return None

def apply_neon_theme():
    st.markdown("""
        <style>

        /* GLOBAL BACKGROUND */
        .stApp {
            background-color: #0a0f1f !important;
            color: #e6e6e6 !important;
            font-family: 'JetBrains Mono', monospace !important;
        }

        /* SCROLLBAR */
        ::-webkit-scrollbar {
            width: 8px;
        }
        ::-webkit-scrollbar-thumb {
            background: #11ffee44;
            border-radius: 10px;
        }

        /* HEADERS */
        h1, h2, h3, h4 {
            font-family: 'JetBrains Mono', monospace !important;
            color: #11f7ff !important;
            text-shadow: 0 0 10px #11ffeeaa;
        }

        /* SIDEBAR */
        section[data-testid="stSidebar"] {
            background-color: #0d1229 !important;
            border-right: 1px solid #11ffef33;
        }

        /* BUTTONS */
        div.stButton > button {
            background-color: #0b132b;
            border: 1px solid #11ffeeaa;
            color: #11f7ff;
            padding: 0.6rem 1rem;
            border-radius: 8px;
            font-weight: bold;
            transition: 0.3s;
        }

        div.stButton > button:hover {
            background-color: #11ffee22;
            color: white;
            border-color: #11ffee;
            box-shadow: 0 0 15px #11ffeeaa;
        }

        /* TEXT AREA + INPUTS */
        textarea, input {
            background-color: #111729 !important;
            color: #c0faff !important;
            border-radius: 6px !important;
            border: 1px solid #11ffee44 !important;
        }

        /* DATAFRAME TABLE */
        .stDataFrame, .dataframe {
            background-color: #0d1229 !important;
            color: #c6f7ff !important;
            border: 1px solid #11ffee33 !important;
        }

        /* EXPANDERS */
        .streamlit-expanderHeader {
            color: #11f7ff !important;
            font-weight: bold;
            text-shadow: 0 0 5px #11ffee;
        }

        /* INFO BOXES */
        .stAlert {
            background-color: #112033 !important;
            border-left: 3px solid #11ffeeaa !important;
            color: #c6faff !important;
        }

        </style>
    """, unsafe_allow_html=True)

# def main():
#     require_login()
#     apply_neon_theme()
#     st.title("🤖 AI-Powered SQL Query Assistant")
#     st.markdown("Ask questions in natural language, and I will generate SQL queries for you to review and run!")
#     st.markdown("---")


#     st.sidebar.title("💡 Example Questions")
#     st.sidebar.markdown("""
#     Try asking questions like:

#     **Anime stats:**
#     - What are the top 10 highest-rated anime?
#     - Show average user score by genre.
#     - List the most popular anime by studio.

#     **User behavior:**
#     - How many users are from each country?
#     - What is the distribution of watch status (Completed, Watching, etc.)?
#     - For each age group, what is the average user score?

#     **Combined:**
#     - For each genre, show the top 5 anime by average user score.
#     """)
#     st.sidebar.markdown("---")
#     st.sidebar.info("""
#         🩼**How it works:**
#         1. Enter your question in plain English
#         2. AI generates SQL query
#         3. Review and optionally edit the query
#         4. Click "Run Query" to execute           
#     """)

#     st.sidebar.markdown("---")
#     if st.sidebar.button("🚪Logout"):
#         st.session_state.logged_in = False
#         st.rerun()

#     # Init state

#     if 'query_history' not in st.session_state:
#         st.session_state.query_history = []
#     if 'generated_sql' not in st.session_state:
#         st.session_state.generated_sql = None
#     if 'current_question' not in st.session_state:
#         st.session_state.current_question = None


#     # main input

#     user_question = st.text_area(
#     "What would you like to know?",
#     height=100,
#     placeholder="Example: Show the top 10 anime by average user rating, with their genres and studios.",
#     )

#     col1, col2, col3 = st.columns([1, 1, 4])
    
#     with col1:
#         generate_button = st.button(" Generate SQL", type="primary", width="stretch")

#     with col2:
#         if st.button(" Clear History", width="stretch"):
#             st.session_state.query_history = []
#             st.session_state.generated_sql = None
#             st.session_state.current_question = None

#     if generate_button and user_question:
#         user_question = user_question.strip()

#         if st.session_state.current_question != user_question:
#             st.session_state.generated_sql = None
#             st.session_state.current_question = None
            


#         with st.spinner("🧠 AI is thinking and generating SQL..."):
#             sql_query = generate_sql_with_gpt(user_question)
#             if sql_query:        
#                 st.session_state.generated_sql = sql_query
#                 st.session_state.current_question = user_question

#     if st.session_state.generated_sql:
#         st.markdown("---")
#         st.subheader("Generated SQL Query")
#         st.info(f"**Question:** {st.session_state.current_question}")

#         edited_sql = st.text_area(
#             "Review and edit the SQL query if needed:", 
#             value=st.session_state.generated_sql,
#             height=200,
#         )

#         col1, col2 = st.columns([1, 5])

#         with col1:
#             run_button = st.button("Run Query", type="primary", width="stretch")

#         if run_button:
#             with st.spinner("Executing query ..."):
#                 df = run_query(edited_sql)
                
#                 if df is not None:
#                     st.session_state.query_history.append(
#                         {'question': user_question, 
#                         'sql': edited_sql, 
#                         'rows': len(df)}
#                     )

#                     st.markdown("---")
#                     st.subheader("📊 Query Results")
#                     st.success(f"✅ Query returned {len(df)} rows")
#                     st.dataframe(df, width="stretch")


#     if st.session_state.query_history:
#         st.markdown('---')
#         st.subheader("📜 Query History")
#         for idx, item in enumerate(reversed(st.session_state.query_history[-5:])):
#             with st.expander(f"Query {len(st.session_state.query_history)-idx}: {item['question'][:60]}..."):
#                 st.markdown(f"**Question:** {item['question']}")
#                 st.code(item["sql"], language="sql")
#                 st.caption(f"Returned {item['rows']} rows")
#                 if st.button(f"Re-run this query", key=f"rerun_{idx}"):
#                     df = run_query(item["sql"])
#                     if df is not None:
#                         st.dataframe(df, width="stretch")


# if __name__ == "__main__":
#     main()
def main():
    require_login()
    apply_neon_theme()

    st.title("🤖 AI-Powered SQL Query Assistant")
    st.markdown(
        "Ask questions in natural language, and I will generate SQL queries for you to review and run!"
    )
    st.markdown("---")

    # ---------- SIDEBAR ----------
    st.sidebar.title("💡 Example Questions")
    st.sidebar.markdown("""
    Try asking questions like:

    **Anime stats:**
    - What are the top 10 highest-rated anime?
    - Show average user score by genre.
    - List the most popular anime by studio.

    **User behavior:**
    - How many users are from each country?
    - What is the distribution of watch status (Completed, Watching, etc.)?
    - For each age group, what is the average user score?

    **Combined:**
    - For each genre, show the top 5 anime by average user score.
    """)
    st.sidebar.markdown("---")
    st.sidebar.info("""
        🩼**How it works:**
        1. Enter your question in plain English  
        2. AI generates SQL query  
        3. Review and optionally edit the query  
        4. Click "Run Query" to execute           
    """)
    st.sidebar.markdown("---")

    if st.sidebar.button("🚪Logout"):
        st.session_state.logged_in = False
        st.rerun()

    # ---------- SESSION STATE ----------
    if "query_history" not in st.session_state:
        st.session_state.query_history = []   # list of {question, sql, rows, df}
    if "current_question" not in st.session_state:
        st.session_state.current_question = None
    if "generated_sql" not in st.session_state:
        st.session_state.generated_sql = None
    if "last_df" not in st.session_state:
        st.session_state.last_df = None
    if "last_rows" not in st.session_state:
        st.session_state.last_rows = 0
    if "typewriter_done" not in st.session_state:
        st.session_state.typewriter_done = True

    # ---------- TABS ----------
    tab_chat, tab_history = st.tabs(["💬 Chat", "📜 Query History"])

    # ========== TAB 1: CHAT ==========
    with tab_chat:
        top_col1, top_col2 = st.columns([3, 1])
        with top_col2:
            if st.button("🧹 Clear History"):
                st.session_state.query_history = []
                st.session_state.current_question = None
                st.session_state.generated_sql = None
                st.session_state.last_df = None
                st.session_state.last_rows = 0
                st.session_state.typewriter_done = True
                st.session_state.pop("sql_editor", None)
                st.rerun()

        # --- Generated SQL + Run button at the top ---
        if st.session_state.generated_sql:
            st.markdown("---")
            st.subheader("🧠 Generated SQL Query")
            st.info(f"**Question:** {st.session_state.current_question}")

            # TYPEWRITER DISPLAY
            sql_text = st.session_state.generated_sql or ""
            if not st.session_state.typewriter_done:
                placeholder = st.empty()
                typed = ""
                for ch in sql_text:
                    typed += ch
                    placeholder.code(typed, language="sql")
                    time.sleep(0.01)
                st.session_state.typewriter_done = True
            else:
                st.code(sql_text, language="sql")

            # Editable SQL area
            if "sql_editor" not in st.session_state:
                st.session_state.sql_editor = sql_text

            edited_sql = st.text_area(
                "Review and edit the SQL query if needed:",
                key="sql_editor",
                height=200,
            )

            run_button = st.button(
                "▶ Run Query", type="primary", use_container_width=True
            )

            if run_button:
                sql_to_run = st.session_state.get("sql_editor", sql_text)
                with st.spinner("Executing query ..."):
                    df = run_query(sql_to_run)
                if df is not None:
                    st.session_state.last_df = df
                    st.session_state.last_rows = len(df)

                    # save to history
                    st.session_state.query_history.append(
                        {
                            "question": st.session_state.current_question,
                            "sql": sql_to_run,
                            "rows": len(df),
                            "df": df,
                        }
                    )
                    st.success(f"✅ Query returned {len(df)} rows")

        # --- Show last results below the SQL block ---
        if st.session_state.last_df is not None:
            st.markdown("---")
            st.subheader("📊 Query Results")
            st.dataframe(st.session_state.last_df, use_container_width=True)

        # --- Input bar at the bottom with arrow icon (ChatGPT-style) ---
        user_question = st.chat_input(
            "Ask something about your anime data (e.g., top 10 highest-rated anime)..."
        )

        if user_question:
            q = user_question.strip()
            if q:
                with st.spinner("🧠 AI is thinking and generating SQL..."):
                    sql_query = generate_sql_with_gpt(q)

                st.session_state.current_question = q
                st.session_state.generated_sql = sql_query or ""
                st.session_state.sql_editor = sql_query or ""
                st.session_state.last_df = None
                st.session_state.last_rows = 0
                st.session_state.typewriter_done = False
                st.rerun()

    # ========== TAB 2: QUERY HISTORY ==========
    with tab_history:
        st.subheader("📜 Query History")

        if not st.session_state.query_history:
            st.info("No queries yet. Ask something in the **Chat** tab first.")
        else:
            for idx, item in enumerate(
                reversed(st.session_state.query_history), start=1
            ):
                label = f"Q{len(st.session_state.query_history)-idx+1}: {item['question'][:70]}..."
                with st.expander(label):
                    st.markdown(f"**Question:** {item['question']}")
                    st.code(item["sql"], language="sql")
                    st.caption(f"Returned `{item['rows']}` rows")

                    if st.button("Re-run this query", key=f"rerun_{idx}"):
                        df = run_query(item["sql"])
                        if df is not None:
                            st.dataframe(df, use_container_width=True)


if __name__ == "__main__":
    main()
