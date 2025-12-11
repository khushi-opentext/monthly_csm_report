from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify, send_file
import psycopg2
from psycopg2.extras import RealDictCursor
import json
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from functools import wraps
import os
from decimal import Decimal
import re
import csv
from io import StringIO, BytesIO


# Import PPT generation functions from separate module (unchanged)
from ppt_generator import get_db_connection_for_ppt, fetch_data, prepare_data_dictionary, generate_presentation

app = Flask(__name__)
app.secret_key = 'your_secret_key_here_change_in_production'

# Session configuration - 30 minute timeout
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(minutes=30)
app.config['SESSION_COOKIE_SECURE'] = False  # Set to True if using HTTPS
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['SESSION_REFRESH_EACH_REQUEST'] = False  # KEY CHANGE: Don't auto-refresh

@app.template_filter('format_month_display')
def format_month_display(date_string):
    """Convert date string to 'Month YYYY' format"""
    try:
        if isinstance(date_string, str):
            dt = datetime.strptime(date_string.split()[0], '%Y-%m-%d')
            return dt.strftime('%B %Y')
        return date_string
    except:
        return date_string

# Database configuration (kept as you provided)
DB_CONFIG = {
    'dbname': 'AutomationDB',
    'host': '10.193.131.151',
    'port': '5432'
}

def get_db_connection():
    """Establishes a connection to the PostgreSQL database using session credentials."""
    try:
        username = session.get('username')
        password = session.get('password')
        if not username or not password:
            raise psycopg2.Error("Missing user credentials in session.")

        conn = psycopg2.connect(
            dbname=DB_CONFIG['dbname'],
            user=username,
            password=password,
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port']
        )
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'username' not in session:
            flash('Session Time out. Please log in to access this page.', 'warning')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

@app.before_request
def before_request_handler():
    """Handle session timeout and logging - with proper inactivity detection"""
    
    # Skip session checks for static files and login page
    if request.endpoint in ['static', 'login', 'check_session']:
        return
    
    # Check if user is logged in
    if 'username' in session:
        # Get last activity time
        last_activity = session.get('last_activity')
        
        if last_activity:
            # Convert string back to datetime if needed
            if isinstance(last_activity, str):
                last_activity = datetime.fromisoformat(last_activity)
            
            # Check if session has expired (30 minutes of inactivity)
            time_since_activity = datetime.now() - last_activity
            
            if time_since_activity > timedelta(minutes=30):
                # Session expired due to inactivity
                session.clear()
                flash('Your session has expired due to inactivity. Please log in again.', 'warning')
                return redirect(url_for('login'))
        
        # Update last activity time for non-check_session requests
        session['last_activity'] = datetime.now().isoformat()
        session.modified = True
        
        # Make session permanent
        session.permanent = True
    
    # Log request with username
    username = session.get("username", "ANONYMOUS")
    print(f"[USER={username}] {request.remote_addr} requested {request.method} {request.path}")


@app.route('/check_session')
def check_session():
    """
    Endpoint to check if session is still valid WITHOUT extending the session.
    This prevents the check itself from counting as activity.
    """
    if 'username' not in session:
        return jsonify({'valid': False}), 401
    
    # Check if session has expired
    last_activity = session.get('last_activity')
    
    if last_activity:
        if isinstance(last_activity, str):
            last_activity = datetime.fromisoformat(last_activity)
        
        time_since_activity = datetime.now() - last_activity
        
        if time_since_activity > timedelta(minutes=30):
            # Session expired
            session.clear()
            return jsonify({'valid': False, 'reason': 'expired'}), 401
        
        # Calculate remaining time
        remaining_seconds = int((timedelta(minutes=30) - time_since_activity).total_seconds())
        
        return jsonify({
            'valid': True,
            'remaining_seconds': remaining_seconds,
            'expires_in': f"{remaining_seconds // 60} minutes"
        }), 200
    
    return jsonify({'valid': True}), 200

@app.route('/logout')
def logout():
    session.clear()   # clears all stored filters also
    flash("Logged out successfully!", "success")
    return redirect(url_for('login'))

def validate_notes_limits(notes_json_text):
    """
    notes_json_text: string that contains a JSON-like mapping for colors,
    e.g. '{"color1": "line1\nline2", "color2":"...", "invalid": ""}' or using single quotes.
    Returns: (True, None) if valid, else (False, "error message")
    """
    if not notes_json_text:
        return True, None

    # tolerate single quotes (the UI sometimes sends single-quoted JSON)
    normalized = notes_json_text.strip()
    if normalized.startswith("'") or "'" in normalized and '"' not in normalized:
        normalized = normalized.replace("'", '"')

    try:
        data = json.loads(normalized)
    except Exception as e:
        return False, f"Notes JSON parse error: {str(e)}"

    # keys we expect may vary; validate any string values found
    for key, val in data.items():
        if val is None:
            continue
        if not isinstance(val, str):
            # if not str, convert to str for safety
            val = str(val)

        # split into lines using \n
        lines = val.splitlines()
        if len(lines) > 3:
            return False, f"'{key}' has {len(lines)} lines (max 3 allowed)."
        # check each line length
        for idx, line in enumerate(lines, start=1):
            # count characters
            if len(line) > 70:
                return False, f"'{key}' line {idx} is {len(line)} chars (max 70)."

    return True, None
# --- end helper ---


@app.route('/')
def index():
    # ALWAYS redirect to login if not logged in
    if 'username' not in session:
        return redirect(url_for('login'))
    return redirect(url_for('metrics'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        
        print(f"[LOGIN ATTEMPT] username={username}")
        try:
            test_conn = psycopg2.connect(
                dbname=DB_CONFIG['dbname'],
                user=username,
                password=password,
                host=DB_CONFIG['host'],
                port=DB_CONFIG['port']
            )
            test_conn.close()
            
            print(f"[LOGIN SUCCESS] username={username}")
            
            # Clear any existing session data
            session.clear()
            
            # Set new session data
            session['username'] = username
            session['password'] = password
            session['last_activity'] = datetime.now().isoformat()
            session.permanent = True  # Enable permanent session
            
            flash('Login successful!', 'success')
            return redirect(url_for('metrics'))
        except psycopg2.Error as e:
            print(f"[LOGIN FAILED] username={username}")
            flash('Invalid database credentials. Access denied.', 'danger')
        except Exception as e:
            flash(f'Error during login: {str(e)}', 'danger')
    
    return render_template('login.html')


@app.route('/metrics', methods=['GET', 'POST'])
@login_required
def metrics():
    # If page load is fresh (F5) – clear stored filters
    if request.args.get("reload") == "1":
        session.pop('reporting_selected_customer', None)
        session.pop('reporting_selected_month', None)
        session.pop('reporting_prev_months', None)
        session.pop('metrics_selected_customer', None)
        session.pop('metrics_selected_month', None)

    conn = get_db_connection()
    if not conn:
        flash('Database connection failed.', 'danger')
        return render_template('metrics.html', customers=[], no_of_envs=2)

    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Load customer list
        cur.execute("""
            SELECT DISTINCT f.customer_name,
                cm.customer_full_name
            FROM final_computed_table f
            LEFT JOIN customer_mapping_table cm
            ON f.customer_name = cm.customer_name
            ORDER BY f.customer_name
        """)
        customers = []
        for row in cur.fetchall():
            customers.append({
                "name": row.get("customer_name"),
                "full": (row.get("customer_full_name") or "")
            })


        # Defaults
        data = None
        selected_customer = None
        selected_month = None
        sel_month_date = None

        # Default config values
        config = {
            "customer_full_name": "",
            "csm_primary": "",
            "csm_secondary": "",
            "customer_uid": [],
            "no_of_environments": 2,
            "no_of_months": "",
            "color_map_thresholds_availability": "",
            "color_map_thresholds_users": "",
            "color_map_thresholds_storage": "",
            "indicator_color_code_rules": "",
            "circle_color_code_rules": "",
            "notes_availability": "",
            "notes_users": "",
            "notes_storage": "",
            "customer_note": ""
        }

        # GET selected customer & month
        if request.method == 'POST':
            selected_customer = request.form.get('customer') or None
            selected_month = request.form.get('month') or None 
            session['metrics_selected_customer'] = selected_customer
            session['metrics_selected_month'] = selected_month
        else:
            selected_customer = session.get('metrics_selected_customer')
            selected_month = session.get('metrics_selected_month')

        if selected_customer and selected_month:
            # Convert month to date
            try:
                sel_month_date = datetime.strptime(selected_month, '%Y-%m-%d').date()
            except:
                sel_month_date = None

            # Load main metrics
            cur.execute("""
                SELECT *
                FROM final_computed_table
                WHERE customer_name = %s AND month_year = %s
            """, (selected_customer, selected_month))
            data = cur.fetchone()

            # Load config fields
            cur.execute("""
                SELECT *
                FROM customer_mapping_table
                WHERE customer_name = %s AND month_year = %s
                LIMIT 1
            """, (selected_customer, sel_month_date))
            row = cur.fetchone()

            if row:
                config = row  # dictionary of fields

        cur.close()
        conn.close()

        # Always return ONE unified render_template
        return render_template(
            'metrics.html',
            customers=customers,
            data=data,
            selected_customer=selected_customer,
            selected_month=selected_month,

            # Config UI variables
            customer_full_name=config["customer_full_name"],
            csm_primary=config["csm_primary"],
            csm_secondary=config["csm_secondary"],
            customer_uid=config["customer_uid"],
            no_of_envs=config["no_of_environments"],
            no_of_months=config["no_of_months"],

            color_map_thresholds_availability=config["color_map_thresholds_availability"],
            color_map_thresholds_users=config["color_map_thresholds_users"],
            color_map_thresholds_storage=config["color_map_thresholds_storage"],

            indicator_color_code_rules=config["indicator_color_code_rules"],
            circle_color_code_rules=config["circle_color_code_rules"],

            notes_availability=config["notes_availability"],
            notes_users=config["notes_users"],
            notes_storage=config["notes_storage"],
            customer_note=config["customer_note"]
        )

    except Exception as e:
        flash(f'Error: {str(e)}', 'danger')
        return render_template('metrics.html', customers=[], no_of_envs=2)


@app.route('/get_months/<customer>')
@login_required
def get_months(customer):
    conn = get_db_connection()
    if not conn:
        return jsonify([])
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT DISTINCT month_year 
            FROM final_computed_table 
            WHERE customer_name = %s 
            ORDER BY month_year DESC
        """, (customer,))
        months = []
        for row in cur.fetchall():
            v = row['month_year']
            # ensure string format YYYY-MM-DD
            if isinstance(v, (datetime, date)):
                months.append(v.strftime('%Y-%m-%d'))
            else:
                months.append(str(v))
        cur.close()
        conn.close()
        return jsonify(months)
    except Exception as e:
        return jsonify([])

@app.route('/save_availability', methods=['POST'])
@login_required
def save_availability():
    try:
        customer = request.form.get('customer')
        month = request.form.get('month')
        availability = float(request.form.get('availability'))
        target = float(request.form.get('target'))
        if availability > 100 or target > 100:
            return jsonify({'success': False, 'message': 'Values must be ≤ 100'})
        availability_decimal = availability / 100
        target_decimal = target / 100
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            UPDATE final_computed_table 
            SET updated_availability = %s, updated_target = %s
            WHERE customer_name = %s AND month_year = %s
        """, (availability_decimal, target_decimal, customer, month))
        cur.execute("""
            UPDATE availability_table 
            SET updated_availability = %s, updated_target = %s
            WHERE customer_name = %s AND month_year = %s
        """, (availability_decimal, target_decimal, customer, month))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'success': True, 'message': f'Availability updated to {availability}% and Target to {target}%'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/save_users', methods=['POST'])
@login_required
def save_users():
    try:
        customer = request.form.get('customer')
        month = request.form.get('month')
        prod_limit = int(request.form.get('prod_limit'))
        prod_used = int(request.form.get('prod_used'))
        test_limit = int(request.form.get('test_limit'))
        test_used = int(request.form.get('test_used'))
        dev_limit = int(request.form.get('dev_limit', 0))
        dev_used = int(request.form.get('dev_used', 0))
        warnings = []
        if prod_used > prod_limit:
            warnings.append('Prod Used > Prod Limit')
        if test_used > test_limit:
            warnings.append('Test Used > Test Limit')
        if dev_used > dev_limit and dev_limit > 0:
            warnings.append('Dev Used > Dev Limit')
        conn = get_db_connection()
        cur = conn.cursor()

        # Update the single record for the specified month
        cur.execute("""
            UPDATE final_computed_table 
            SET updated_prod_limit = %s, updated_prod_used = %s,
                updated_test_limit = %s, updated_test_used = %s,
                updated_dev_limit = %s, updated_dev_used = %s
            WHERE customer_name = %s AND month_year = %s
        """, (prod_limit, prod_used, test_limit, test_used, dev_limit, dev_used, customer, month))

        # Propagate the new limits to all future months for that customer
        cur.execute("""
            UPDATE final_computed_table
            SET updated_prod_limit = %s, updated_test_limit = %s, updated_dev_limit = %s
            WHERE customer_name = %s AND month_year > %s
        """, (prod_limit, test_limit, dev_limit, customer, month))

        # Also update the underlying users_table for the specified month
        cur.execute("""
            UPDATE users_table 
            SET updated_prod_limit = %s, updated_prod_used = %s,
                updated_test_limit = %s, updated_test_used = %s,
                updated_dev_limit = %s, updated_dev_used = %s
            WHERE customer_name = %s AND month_year = %s
        """, (prod_limit, prod_used, test_limit, test_used, dev_limit, dev_used, customer, month))

        # And propagate the limits in the users_table as well
        cur.execute("""
            UPDATE users_table
            SET updated_prod_limit = %s, updated_test_limit = %s, updated_dev_limit = %s
            WHERE customer_name = %s AND month_year > %s
        """, (prod_limit, test_limit, dev_limit, customer, month))

        conn.commit()
        cur.close()
        conn.close()
        message = 'Users data updated successfully'
        if warnings:
            message += ' (Warning: ' + ', '.join(warnings) + ')'
        return jsonify({'success': True, 'message': message, 'warnings': warnings})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/save_storage', methods=['POST'])
@login_required
def save_storage():
    try:
        def to_decimal(val, default=Decimal('0.0')):
            if val is None or val == '':
                return default
            try:
                return Decimal(str(val))
            except Exception:
                try:
                    return Decimal(str(float(val)))
                except Exception:
                    return default

        customer = request.form.get('customer')
        month = request.form.get('month')
        prod_target = to_decimal(request.form.get('prod_target'))
        prod_actual = to_decimal(request.form.get('prod_actual'))
        test_target = to_decimal(request.form.get('test_target'))
        test_actual = to_decimal(request.form.get('test_actual'))
        dev_target = to_decimal(request.form.get('dev_target', 0))
        dev_actual = to_decimal(request.form.get('dev_actual', 0))
        conn = get_db_connection()
        cur = conn.cursor()

        # Update the single record for the specified month (including actual usage)
        cur.execute("""
            UPDATE final_computed_table 
            SET updated_prod_target_storage_gb = %s, updated_prod_storage_gb = %s,
                updated_test_target_storage_gb = %s, updated_test_storage_gb = %s,
                updated_dev_target_storage_gb = %s, updated_dev_storage_gb = %s
            WHERE customer_name = %s AND month_year = %s
        """, (prod_target, prod_actual, test_target, test_actual, dev_target, dev_actual, customer, month))

        # Propagate the new storage targets to all future months for that customer
        cur.execute("""
            UPDATE final_computed_table
            SET updated_prod_target_storage_gb = %s, updated_test_target_storage_gb = %s, updated_dev_target_storage_gb = %s
            WHERE customer_name = %s AND month_year > %s
        """, (prod_target, test_target, dev_target, customer, month))

        # Also update the underlying storage_table for the specified month
        cur.execute("""
            UPDATE storage_table 
            SET updated_prod_target_storage_gb = %s, updated_prod_storage_gb = %s,
                updated_test_target_storage_gb = %s, updated_test_storage_gb = %s,
                updated_dev_target_storage_gb = %s, updated_dev_storage_gb = %s
            WHERE customer_name = %s AND month_year = %s
        """, (prod_target, prod_actual, test_target, test_actual, dev_target, dev_actual, customer, month))

        # And propagate the storage targets in the storage_table as well
        cur.execute("""
            UPDATE storage_table
            SET updated_prod_target_storage_gb = %s, updated_test_target_storage_gb = %s, updated_dev_target_storage_gb = %s
            WHERE customer_name = %s AND month_year > %s
        """, (prod_target, test_target, dev_target, customer, month))

        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'success': True, 'message': 'Storage data updated successfully'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/save_tickets', methods=['POST'])
@login_required
def save_tickets():
    try:
        data = request.get_json()

        customer = data.get('customer')
        month = data.get('month')

        opened = int(data.get('opened'))
        closed = int(data.get('closed'))
        curr_backlog = int(data.get('curr_backlog'))
        overall_backlog = int(data.get('overall_backlog'))

        conn = get_db_connection()
        cur = conn.cursor()

        # 1️⃣ UPDATE final_computed_table  (your system depends on this)
        cur.execute("""
            UPDATE final_computed_table
            SET 
                updated_current_opened_tickets = %s,
                updated_current_closed_tickets = %s,
                updated_current_backlog_tickets = %s,
                updated_tickets_backlog = %s
            WHERE customer_name = %s
              AND month_year = %s::date
        """, (
            opened,
            closed,
            curr_backlog,
            overall_backlog,
            customer,
            month
        ))

        # 2️⃣ UPDATE tickets_computed_table  (THIS is what UI loads)
        cur.execute("""
            UPDATE tickets_computed_table
            SET 
                updated_current_opened_tickets = %s,
                updated_current_closed_tickets = %s,
                updated_current_backlog_tickets = %s,
                updated_tickets_backlog = %s
            WHERE customer_name = %s
              AND month_year = %s::date
        """, (
            opened,
            closed,
            curr_backlog,
            overall_backlog,
            customer,
            month
        ))

        conn.commit()
        cur.close()
        conn.close()

        return jsonify({'success': True, 'message': 'Tickets updated successfully'})

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/generate_ppt', methods=['POST'])
@login_required
def generate_ppt():
    try:
        customer = request.form.get('customer')
        month = request.form.get('month')
        conn = get_db_connection_for_ppt(session['username'], session['password'])
        if not conn:
            return jsonify({'success': False, 'message': 'Database connection failed'})
        customer_mapping_df, final_computed_df = fetch_data(conn, customer, month)
        conn.close()
        if not customer_mapping_df.empty and not final_computed_df.empty:
            data_dict = prepare_data_dictionary(customer_mapping_df, final_computed_df, month)
            #output_filename = f"{customer}_{month.replace('-', '_')}.pptx"
            dt = datetime.strptime(month, "%Y-%m-%d")   # month = "2025-08-01"
            year = dt.strftime("%Y")                    # "2025"
            mon  = dt.strftime("%b")                    # "Aug"
            output_filename = f"{customer}_{year}_{mon}.pptx"
            generate_presentation(data_dict, output_filename)
            response = send_file(output_filename, as_attachment=True)
            @response.call_on_close
            def cleanup():
                try:
                    if os.path.exists(output_filename):
                        os.remove(output_filename)
                except Exception as e:
                    print(f"Error deleting file: {e}")
            return response
        else:
            return jsonify({'success': False, 'message': 'No data found for PPT generation'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

def fetch_reporting_data(cur, selected_customer, selected_month, prev_months):
    """
    Helper to fetch reporting rows from final_computed_table
    for a customer + month range.

    cur  : a RealDictCursor
    selected_customer : customer_name string
    selected_month    : 'YYYY-MM-DD' string (represents the end month)
    prev_months       : int number of months to go back (inclusive)
    """
    if not selected_customer or not selected_month:
        return []

    try:
        # selected_month is the END month in the range
        end_date = datetime.strptime(selected_month, '%Y-%m-%d').date()
    except ValueError:
        # invalid month format – return no data
        return []

    # Start date = end_date minus (prev_months - 1) months
    start_date = end_date - relativedelta(months=prev_months - 1)

    cur.execute("""
        SELECT *
        FROM final_computed_table 
        WHERE customer_name = %s 
          AND month_year BETWEEN %s AND %s
        ORDER BY month_year
    """, (selected_customer, start_date, end_date))

    return cur.fetchall()



@app.route('/reporting', methods=['GET', 'POST'])
@login_required
def reporting():
    """
    Reporting page:
     - loads customers list
     - reads prev_months, selected customer/month
     - queries final_computed_table for the date range
     - determines no_of_envs from customer_mapping_table (fallback to latest)
     - converts rows into JSON-serializable dicts before rendering
    """
    # If page load is fresh (F5) – clear stored filters
    if request.args.get("reload") == "1":
        session.pop('reporting_selected_customer', None)
        session.pop('reporting_selected_month', None)
        session.pop('reporting_prev_months', None)
        session.pop('metrics_selected_customer', None)
        session.pop('metrics_selected_month', None)

    conn = get_db_connection()
    if not conn:
        flash('Database connection failed.', 'danger')
        return render_template('reporting.html', customers=[])

    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT DISTINCT customer_name FROM final_computed_table ORDER BY customer_name")
        customers = [row['customer_name'] for row in cur.fetchall()]

        data = []
        if request.method == 'POST':
            selected_customer = request.form.get('customer') or None
            selected_month = request.form.get('month') or None 
            prev_months_raw = request.form.get('prev_months')
            try:
                prev_months = int(prev_months_raw)
            except (TypeError, ValueError):
                prev_months = 6
            prev_months = max(1, min(24, prev_months))
            session['reporting_selected_customer'] = selected_customer
            session['reporting_selected_month'] = selected_month
            session['reporting_prev_months'] = prev_months
        else:
            selected_customer = session.get('reporting_selected_customer')
            selected_month = session.get('reporting_selected_month')
            prev_months = session.get('reporting_prev_months', 6)

        if selected_customer and selected_month:
            data = fetch_reporting_data(cur, selected_customer, selected_month, prev_months)
        else:
            data = []

        # determine no_of_envs (default 2)
        no_of_envs = 2
        try:
            if selected_customer:
                sel_month_date = None
                if selected_month:
                    try:
                        sel_month_date = datetime.strptime(selected_month, '%Y-%m-%d').date()
                    except Exception:
                        sel_month_date = None

                if sel_month_date:
                    cur.execute("""
                        SELECT no_of_environments
                        FROM customer_mapping_table
                        WHERE customer_name = %s AND month_year = %s
                        LIMIT 1
                    """, (selected_customer, sel_month_date))
                    row = cur.fetchone()
                    if row and row.get('no_of_environments') is not None:
                        no_of_envs = int(row.get('no_of_environments') or 2)
                    else:
                        cur.execute("""
                            SELECT no_of_environments
                            FROM customer_mapping_table
                            WHERE customer_name = %s
                            ORDER BY month_year DESC
                            LIMIT 1
                        """, (selected_customer,))
                        row2 = cur.fetchone()
                        if row2 and row2.get('no_of_environments') is not None:
                            no_of_envs = int(row2.get('no_of_environments') or 2)
                else:
                    cur.execute("""
                        SELECT no_of_environments
                        FROM customer_mapping_table
                        WHERE customer_name = %s
                        ORDER BY month_year DESC
                        LIMIT 1
                    """, (selected_customer,))
                    row_latest = cur.fetchone()
                    if row_latest and row_latest.get('no_of_environments') is not None:
                        no_of_envs = int(row_latest.get('no_of_environments') or 2)
        except Exception:
            no_of_envs = 2

        # Convert rows (RealDictRow) to JSON-serializable plain dicts
        def serializable_value(v):
            if v is None:
                return None
            if isinstance(v, (datetime, date)):
                return v.strftime('%Y-%m-%d')
            if isinstance(v, Decimal):
                # convert to int if integral, otherwise float
                try:
                    if v == v.to_integral_value():
                        return int(v)
                except Exception:
                    pass
                try:
                    return float(v)
                except Exception:
                    return str(v)
            # basic types OK
            if isinstance(v, (int, float, str, bool)):
                return v
            # bytes
            if isinstance(v, (bytes, bytearray)):
                try:
                    return v.decode('utf-8', errors='ignore')
                except Exception:
                    return str(v)
            # fallback to string
            try:
                return str(v)
            except Exception:
                return None

        data_serializable = []
        for row in (data or []):
            d = {}
            # row is dict-like (RealDictRow), iterate keys
            for k in row.keys():
                try:
                    d[k] = serializable_value(row.get(k))
                except Exception:
                    d[k] = None
            data_serializable.append(d)

        cur.execute("""
            SELECT DISTINCT csm FROM (
                SELECT csm_primary AS csm FROM final_computed_table
                UNION
                SELECT csm_secondary AS csm FROM final_computed_table
            ) AS all_csms
            WHERE csm IS NOT NULL
            ORDER BY csm;
        """)
        csm_list = [row['csm'] for row in cur.fetchall()]

        cur.execute("""
            SELECT DISTINCT TO_CHAR(month_year, 'YYYY-MM') AS month
                FROM final_computed_table
                ORDER BY month DESC
            """)
        available_months = [row['month'] for row in cur.fetchall()]

        cur.close()
        conn.close()

        # pass JSON-serializable data to template
        return render_template('reporting.html',
                               customers=customers,
                               data=data_serializable,
                               selected_customer=selected_customer or '',
                               selected_month=selected_month or '',
                               prev_months=prev_months,
                               no_of_envs=no_of_envs,
                               csm_list=csm_list,
                               available_months=available_months)
    except Exception as e:
        flash(f'Error: {str(e)}', 'danger')
        return render_template('reporting.html', customers=[])

@app.route('/save_config', methods=['POST'])
@login_required
def save_config():
    data = request.get_json()
    print("RAW UI DATA:", data)

    customer = data.get("customer")
    month = data.get("month")

    try:
        month_date = datetime.strptime(month, "%Y-%m-%d").date()
    except:
        return {"success": False, "message": "Invalid month format"}

    def safe_json(value):
        if not value:
            return None
        value = value.strip()
        value = value.replace("'", '"')
        value = value.replace("None", "null").replace("True", "true").replace("False", "false")
        try:
            parsed = json.loads(value)
            return json.dumps(parsed)
        except:
            return None

    thr_availability = safe_json(data.get("thr_availability"))
    thr_users = safe_json(data.get("thr_users"))
    thr_storage = safe_json(data.get("thr_storage"))
    indicator_colors = safe_json(data.get("indicator_colors"))
    circle_colors = safe_json(data.get("circle_colors"))
    notes_availability = safe_json(data.get("notes_availability"))
    notes_users = safe_json(data.get("notes_users"))
    notes_storage = safe_json(data.get("notes_storage"))

    if None in [
        thr_availability, thr_users, thr_storage,
        indicator_colors, circle_colors,
        notes_availability, notes_users, notes_storage
    ]:
        return {"success": False, "message": "One or more JSON fields contain invalid JSON."}
    
    # --- Server-side defensive validation for notes limits (defense-in-depth) ---
    # Validate notes JSONs (max 3 lines per color, max 70 chars per line)
    valid, msg = validate_notes_limits(notes_availability)
    if not valid:
        return {"success": False, "message": f"Availability notes invalid: {msg}"}

    valid, msg = validate_notes_limits(notes_users)
    if not valid:
        return {"success": False, "message": f"Users notes invalid: {msg}"}

    valid, msg = validate_notes_limits(notes_storage)
    if not valid:
        return {"success": False, "message": f"Storage notes invalid: {msg}"}
    # --- end server-side validation ---

    customer_full_name = data.get("customer_full_name", "").strip()
    new_uid = data.get("new_customer_uid", "").strip()

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("""
        SELECT customer_uid
        FROM customer_mapping_table
        WHERE customer_name = %s AND month_year = %s
    """, (customer, month_date))

    row_uid = cur.fetchone()

    if row_uid and row_uid.get("customer_uid"):
        existing_uid_list = list(row_uid["customer_uid"])
    else:
        existing_uid_list = []

    if new_uid:
        existing_uid_list.append(new_uid)

    uid_array = existing_uid_list  # final saved list

    sql = """
        UPDATE customer_mapping_table
        SET
            customer_full_name = %s,
            csm_primary = %s,
            csm_secondary = %s,
            customer_uid = %s,
            no_of_environments = %s,
            no_of_months = %s,
            color_map_thresholds_availability = %s::jsonb,
            color_map_thresholds_users = %s::jsonb,
            color_map_thresholds_storage = %s::jsonb,
            indicator_color_code_rules = %s::jsonb,
            circle_color_code_rules = %s::jsonb,
            notes_availability = %s::jsonb,
            notes_users = %s::jsonb,
            notes_storage = %s::jsonb,
            customer_note = %s
        WHERE customer_name = %s AND month_year = %s
    """

    values = (
        customer_full_name,
        data.get("csm_primary"),
        data.get("csm_secondary"),
        uid_array,
        data.get("no_of_envs"),
        data.get("no_of_months"),
        thr_availability,
        thr_users,
        thr_storage,
        indicator_colors,
        circle_colors,
        notes_availability,
        notes_users,
        notes_storage,
        data.get("customer_note"),
        customer,
        month_date
    )

    sql2 = """UPDATE final_computed_table
              SET csm_primary = %s, csm_secondary = %s
              WHERE customer_name = %s AND month_year = %s"""
    
    cur.execute(sql2, (
        data.get("csm_primary"),data.get("csm_secondary"),
        customer, month_date))

    try:
        cur.execute(sql, values)
        conn.commit()
        cur.close()
        conn.close()
        return {"success": True}

    except Exception as e:
        conn.rollback()
        cur.close()
        conn.close()
        return {"success": False, "message": str(e)}


from datetime import datetime
from flask import jsonify

@app.route('/insert_record', methods=['POST'])
@login_required
def insert_record():
    """
    Insert either:
      - CONFIG only  (customer_mapping_table), or
      - TABLE DATA   (final_computed_table + availability/users/storage/tickets)
    depending on `mode` in the POST body.
    """
    try:
        mode = request.form.get("mode")          # "config" or "table"
        customer = request.form.get("customer")
        month_raw = request.form.get("month")

        if not customer or not month_raw:
            return jsonify({"success": False,
                            "message": "Customer and Month are required."})

        # Accept both "YYYY-MM" and "YYYY-MM-DD"
        month_date = None
        for fmt in ("%Y-%m", "%Y-%m-%d"):
            try:
                month_date = datetime.strptime(month_raw, fmt).date()
                break
            except ValueError:
                continue
        if month_date is None:
            return jsonify({"success": False,
                            "message": "Invalid month format. Use YYYY-MM or YYYY-MM-DD."})

        # ===================== CONFIG MODE =====================
        if mode == "config":
            csm_primary   = request.form.get("csm_primary", "").strip()
            csm_secondary = request.form.get("csm_secondary", "").strip() or csm_primary
            no_of_months  = int(request.form.get("no_of_months", 6))
            no_of_envs    = int(request.form.get("no_of_environments", 2))

            conn = get_db_connection()
            cur = conn.cursor()

            cur.execute("""
                SELECT 1 FROM customer_mapping_table WHERE customer_name = %s
            """, (customer,))
            if cur.fetchone():
                conn.rollback()
                cur.close()
                conn.close()
                return jsonify({"success": False,
                        "message": f"Customer '{customer}' already exists."})

            cur.execute("""
                INSERT INTO customer_mapping_table
                    (customer_name, month_year,
                     csm_primary, csm_secondary,
                     no_of_environments, no_of_months)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (customer_name, month_year)
                DO UPDATE SET
                    csm_primary       = EXCLUDED.csm_primary,
                    csm_secondary     = EXCLUDED.csm_secondary,
                    no_of_environments = EXCLUDED.no_of_environments,
                    no_of_months       = EXCLUDED.no_of_months
            """, (customer, month_date,
                  csm_primary, csm_secondary,
                  no_of_envs, no_of_months))

            # Insert into availability_table
            cur.execute("""
                INSERT INTO availability_table (customer_name, month_year, total_availability, updated_availability, target, updated_target)
                VALUES (%s, %s, 0, 0, 0, 0)
                ON CONFLICT DO NOTHING
            """, (customer, month_date))

            # Insert into users_table
            cur.execute("""
                INSERT INTO users_table (customer_name, month_year,
                             prod_limit, test_limit, dev_limit,
                             prod_used, test_used, dev_used,
                             updated_prod_limit, updated_test_limit, updated_dev_limit,
                             updated_prod_used, updated_test_used, updated_dev_used)
                VALUES (%s, %s, 0,0,0, 0,0,0, 0,0,0, 0,0,0)
                ON CONFLICT DO NOTHING
            """, (customer, month_date))

            # Insert into storage_table
            cur.execute("""
                INSERT INTO storage_table (customer_name, month_year,
                               prod_target_storage_gb, test_target_storage_gb, dev_target_storage_gb,
                               prod_storage_gb, test_storage_gb, dev_storage_gb,
                               updated_prod_target_storage_gb, updated_test_target_storage_gb, updated_dev_target_storage_gb,
                               updated_prod_storage_gb, updated_test_storage_gb, updated_dev_storage_gb)
                VALUES (%s, %s, 0,0,0, 0,0,0, 0,0,0, 0,0,0)
                ON CONFLICT DO NOTHING
            """, (customer, month_date))

            # Insert into tickets_computed_table
            cur.execute("""
                INSERT INTO tickets_computed_table 
                (customer_name, month_year,
                 tickets_opened, tickets_closed, tickets_backlog,
                 updated_tickets_opened, updated_tickets_closed, updated_tickets_backlog,
                 current_opened_tickets, current_closed_tickets, current_backlog_tickets,
                 updated_current_opened_tickets, updated_current_closed_tickets, updated_current_backlog_tickets,
                 p1_opened,p1_closed,p1_backlog,
                 p2_opened,p2_closed,p2_backlog,
                 p3_opened,p3_closed,p3_backlog,
                 p4_opened,p4_closed,p4_backlog,
                 updated_p1_opened, updated_p1_closed, updated_p1_backlog,
                 updated_p2_opened, updated_p2_closed, updated_p2_backlog,
                 updated_p3_opened, updated_p3_closed, updated_p3_backlog,
                 updated_p4_opened, updated_p4_closed, updated_p4_backlog)
            VALUES (%s, %s,
                    0,0,0, 0,0,0, 0,0,0, 0,0,0,
                    0,0,0, 0,0,0, 0,0,0, 0,0,0,
                    0,0,0, 0,0,0, 0,0,0, 0,0,0)
            ON CONFLICT DO NOTHING
            """, (customer, month_date))

            # Insert into final_computed_table
            cur.execute("""
    INSERT INTO final_computed_table (
        customer_name, month_year,
        csm_primary, csm_secondary,

        updated_availability, updated_target,
        
        updated_prod_limit, updated_test_limit, updated_dev_limit,
        updated_prod_used, updated_test_used, updated_dev_used,

        updated_prod_target_storage_gb, updated_test_target_storage_gb, updated_dev_target_storage_gb,
        updated_prod_storage_gb, updated_test_storage_gb, updated_dev_storage_gb,

        updated_tickets_opened, updated_tickets_closed, updated_tickets_backlog,
        updated_current_opened_tickets, updated_current_closed_tickets, updated_current_backlog_tickets,

        updated_p1_opened, updated_p1_closed, updated_p1_backlog,
        updated_p2_opened, updated_p2_closed, updated_p2_backlog,
        updated_p3_opened, updated_p3_closed, updated_p3_backlog,
        updated_p4_opened, updated_p4_closed, updated_p4_backlog,

        customer_full_name, customer_uid
    )
    SELECT
        %s, %s,
        %s, %s,

        COALESCE(a.updated_availability, 0),
        COALESCE(a.updated_target, 0),

        COALESCE(u.updated_prod_limit, 0),
        COALESCE(u.updated_test_limit, 0),
        COALESCE(u.updated_dev_limit, 0),

        COALESCE(u.updated_prod_used, 0),
        COALESCE(u.updated_test_used, 0),
        COALESCE(u.updated_dev_used, 0),

        COALESCE(s.updated_prod_target_storage_gb, 0),
        COALESCE(s.updated_test_target_storage_gb, 0),
        COALESCE(s.updated_dev_target_storage_gb, 0),

        COALESCE(s.updated_prod_storage_gb, 0),
        COALESCE(s.updated_test_storage_gb, 0),
        COALESCE(s.updated_dev_storage_gb, 0),

        COALESCE(t.updated_tickets_opened, 0),
        COALESCE(t.updated_tickets_closed, 0),
        COALESCE(t.updated_tickets_backlog, 0),

        COALESCE(t.updated_current_opened_tickets, 0),
        COALESCE(t.updated_current_closed_tickets, 0),
        COALESCE(t.updated_current_backlog_tickets, 0),

        COALESCE(t.updated_p1_opened, 0),
        COALESCE(t.updated_p1_closed, 0),
        COALESCE(t.updated_p1_backlog, 0),
        COALESCE(t.updated_p2_opened, 0),
        COALESCE(t.updated_p2_closed, 0),
        COALESCE(t.updated_p2_backlog, 0),
        COALESCE(t.updated_p3_opened, 0),
        COALESCE(t.updated_p3_closed, 0),
        COALESCE(t.updated_p3_backlog, 0),
        COALESCE(t.updated_p4_opened, 0),
        COALESCE(t.updated_p4_closed, 0),
        COALESCE(t.updated_p4_backlog, 0),

        NULL,
        ARRAY[]::text[]
    FROM
        (SELECT * FROM availability_table WHERE customer_name=%s AND month_year=%s) a
        FULL OUTER JOIN
        (SELECT * FROM users_table WHERE customer_name=%s AND month_year=%s) u
        ON true
        FULL OUTER JOIN
        (SELECT * FROM storage_table WHERE customer_name=%s AND month_year=%s) s
        ON true
        FULL OUTER JOIN
        (SELECT * FROM tickets_computed_table WHERE customer_name=%s AND month_year=%s) t
        ON true
    WHERE NOT EXISTS (
        SELECT 1 FROM final_computed_table WHERE customer_name=%s AND month_year=%s
    )
""", (
    customer, month_date,      # for final_computed_table insert
    csm_primary, csm_secondary,
    
    customer, month_date,      # availability join
    customer, month_date,      # users join
    customer, month_date,      # storage join
    customer, month_date,      # tickets join

    customer, month_date       # NOT EXISTS check
))

            conn.commit()
            cur.close()
            conn.close()

            return jsonify({"success": True,
                            "message": "Configuration saved successfully."})

        # ===================== TABLE DATA MODE =====================
        if mode == "table":

            # Helper converters
            def to_float(x):
                try:
                    return float(x)
                except (TypeError, ValueError):
                    return 0.0

            def to_int(x):
                try:
                    return int(x)
                except (TypeError, ValueError):
                    return 0

            conn = get_db_connection()
            cur = conn.cursor()

            # 1) Ensure configuration exists
            cur.execute("""
                SELECT csm_primary, csm_secondary
                FROM customer_mapping_table
                WHERE customer_name = %s AND month_year = %s
            """, (customer, month_date))
            mapping = cur.fetchone()
            if not mapping:
                cur.close()
                conn.close()
                return jsonify({
                    "success": False,
                    "message": "Configuration missing! Please enter configuration first."
                })

            csm_primary, csm_secondary = mapping

            # 2) Build payload from form values
            payload = {
                'updated_availability':  to_float(request.form.get('updated_availability')) / 100,
                'updated_target':        to_float(request.form.get('updated_target')) / 100,

                'updated_prod_limit':    to_int(request.form.get('updated_prod_limit')),
                'updated_test_limit':    to_int(request.form.get('updated_test_limit')),
                'updated_dev_limit':     to_int(request.form.get('updated_dev_limit')),
                'updated_prod_used':     to_int(request.form.get('updated_prod_used')),
                'updated_test_used':     to_int(request.form.get('updated_test_used')),
                'updated_dev_used':      to_int(request.form.get('updated_dev_used')),

                'updated_prod_target_storage_gb': to_float(request.form.get('updated_prod_target_storage_gb')),
                'updated_test_target_storage_gb': to_float(request.form.get('updated_test_target_storage_gb')),
                'updated_dev_target_storage_gb':  to_float(request.form.get('updated_dev_target_storage_gb')),
                'updated_prod_storage_gb':        to_float(request.form.get('updated_prod_storage_gb')),
                'updated_test_storage_gb':        to_float(request.form.get('updated_test_storage_gb')),
                'updated_dev_storage_gb':         to_float(request.form.get('updated_dev_storage_gb')),

                'updated_tickets_opened':  to_int(request.form.get('updated_tickets_opened')),
                'updated_tickets_closed':  to_int(request.form.get('updated_tickets_closed')),
                'updated_tickets_backlog': to_int(request.form.get('updated_tickets_backlog')),

                'updated_current_opened_tickets':  to_int(request.form.get('updated_current_opened_tickets')),
                'updated_current_closed_tickets':  to_int(request.form.get('updated_current_closed_tickets')),
                'updated_current_backlog_tickets': to_int(request.form.get('updated_current_backlog_tickets')),
            }

            # 3) Insert into final_computed_table with ON CONFLICT protection
            cur.execute("""
                INSERT INTO final_computed_table
                    (customer_name, month_year, csm_primary, csm_secondary,
                     updated_availability, updated_target,
                     updated_prod_limit, updated_test_limit, updated_dev_limit,
                     updated_prod_used, updated_test_used, updated_dev_used,
                     updated_prod_target_storage_gb, updated_test_target_storage_gb, updated_dev_target_storage_gb,
                     updated_prod_storage_gb, updated_test_storage_gb, updated_dev_storage_gb,
                     updated_tickets_opened, updated_tickets_closed, updated_tickets_backlog,
                     updated_current_opened_tickets, updated_current_closed_tickets, updated_current_backlog_tickets)
                VALUES (%s,%s,%s,%s,
                        %s,%s,
                        %s,%s,%s,
                        %s,%s,%s,
                        %s,%s,%s,
                        %s,%s,%s,
                        %s,%s,%s,
                        %s,%s,%s)
                ON CONFLICT (customer_name, month_year) DO NOTHING
            """, (
                customer, month_date, csm_primary, csm_secondary,
                payload['updated_availability'], payload['updated_target'],
                payload['updated_prod_limit'], payload['updated_test_limit'], payload['updated_dev_limit'],
                payload['updated_prod_used'], payload['updated_test_used'], payload['updated_dev_used'],
                payload['updated_prod_target_storage_gb'], payload['updated_test_target_storage_gb'],
                payload['updated_dev_target_storage_gb'],
                payload['updated_prod_storage_gb'], payload['updated_test_storage_gb'],
                payload['updated_dev_storage_gb'],
                payload['updated_tickets_opened'], payload['updated_tickets_closed'],
                payload['updated_tickets_backlog'],
                payload['updated_current_opened_tickets'], payload['updated_current_closed_tickets'],
                payload['updated_current_backlog_tickets']
            ))

            # If nothing was inserted, the row already exists
            if cur.rowcount == 0:
                conn.rollback()
                cur.close()
                conn.close()
                return jsonify({
                    "success": False,
                    "message": "Table data already exists for this customer & month."
                })

            # 4) Insert into AVAILABILITY table
            cur.execute("""
                INSERT INTO availability_table
                    (customer_name, month_year,
                     total_availability, updated_availability,
                     target, updated_target)
                VALUES (%s,%s,%s,%s,%s,%s)
            """, (
                customer, month_date,
                payload['updated_availability'], payload['updated_availability'],
                payload['updated_target'],       payload['updated_target']
            ))

            # 5) Insert into USERS table
            cur.execute("""
                INSERT INTO users_table
                    (customer_name, month_year,
                     prod_limit, prod_used,
                     test_limit, test_used,
                     dev_limit, dev_used,
                     updated_prod_limit, updated_prod_used,
                     updated_test_limit, updated_test_used,
                     updated_dev_limit, updated_dev_used)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                customer, month_date,
                payload['updated_prod_limit'], payload['updated_prod_used'],
                payload['updated_test_limit'], payload['updated_test_used'],
                payload['updated_dev_limit'],  payload['updated_dev_used'],
                # updated_* are same as base for now
                payload['updated_prod_limit'], payload['updated_prod_used'],
                payload['updated_test_limit'], payload['updated_test_used'],
                payload['updated_dev_limit'],  payload['updated_dev_used']
            ))

            # 6) Insert into STORAGE table
            cur.execute("""
                INSERT INTO storage_table
                    (customer_name, month_year,
                     prod_target_storage_gb, prod_storage_gb,
                     test_target_storage_gb, test_storage_gb,
                     dev_target_storage_gb,  dev_storage_gb,
                     updated_prod_target_storage_gb, updated_prod_storage_gb,
                     updated_test_target_storage_gb, updated_test_storage_gb,
                     updated_dev_target_storage_gb,  updated_dev_storage_gb)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                customer, month_date,
                payload['updated_prod_target_storage_gb'], payload['updated_prod_storage_gb'],
                payload['updated_test_target_storage_gb'], payload['updated_test_storage_gb'],
                payload['updated_dev_target_storage_gb'],  payload['updated_dev_storage_gb'],
                # updated_* same
                payload['updated_prod_target_storage_gb'], payload['updated_prod_storage_gb'],
                payload['updated_test_target_storage_gb'], payload['updated_test_storage_gb'],
                payload['updated_dev_target_storage_gb'],  payload['updated_dev_storage_gb']
            ))

            # 7) Insert into TICKETS table
            cur.execute("""
                INSERT INTO tickets_computed_table
                    (customer_name, month_year,
                     tickets_opened, tickets_closed, tickets_backlog,
                     updated_tickets_opened, updated_tickets_closed, updated_tickets_backlog,
                     current_opened_tickets, current_closed_tickets, current_backlog_tickets,
                     updated_current_opened_tickets, updated_current_closed_tickets,
                     updated_current_backlog_tickets)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                customer, month_date,
                payload['updated_tickets_opened'], payload['updated_tickets_closed'],
                payload['updated_tickets_backlog'],
                payload['updated_tickets_opened'], payload['updated_tickets_closed'],
                payload['updated_tickets_backlog'],
                payload['updated_current_opened_tickets'], payload['updated_current_closed_tickets'],
                payload['updated_current_backlog_tickets'],
                payload['updated_current_opened_tickets'], payload['updated_current_closed_tickets'],
                payload['updated_current_backlog_tickets']
            ))

            conn.commit()
            cur.close()
            conn.close()

            return jsonify({"success": True,
                            "message": "Table data inserted successfully!"})

        # If mode is something else
        return jsonify({"success": False, "message": "Invalid mode."})

    except Exception as e:
        # Generic safety net
        return jsonify({"success": False, "message": str(e)})


@app.route('/audit_logs/latest', methods=['GET'])
@login_required
def audit_logs_latest():
    """
    Return latest 10 audit_logs rows as JSON for the Reporting page modal.
    """
    conn = get_db_connection()
    if not conn:
        return jsonify({"success": False, "message": "Database connection failed"}), 500

    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT
                audit_id,
                table_name,
                operation_type,
                changed_at,
                username,
                old_data,
                new_data,
                section_name,
                comment
            FROM audit_logs
            ORDER BY changed_at DESC
            LIMIT 10;
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()

        return jsonify({"success": True, "rows": rows})
    except Exception as e:
        if conn:
            conn.close()
        return jsonify({"success": False, "message": str(e)}), 500


@app.route('/audit_logs/download', methods=['GET'])
@login_required
def audit_logs_download():
    """
    Download the full audit_logs table as a CSV file.
    """
    conn = get_db_connection()
    if not conn:
        flash('Database connection failed.', 'danger')
        return redirect(url_for('reporting'))

    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                audit_id,
                table_name,
                operation_type,
                -- format changed_at as a plain text timestamp
                TO_CHAR(changed_at, 'YYYY-MM-DD HH24:MI:SS') AS changed_at,
                username,
                old_data,
                new_data,
                section_name,
                comment
            FROM audit_logs
            ORDER BY changed_at DESC;
        """)
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]

        cur.close()
        conn.close()

        # Write to in-memory CSV
        text_buffer = StringIO()
        writer = csv.writer(text_buffer)
        writer.writerow(colnames)
        writer.writerows(rows)
        text_buffer.seek(0)

        # Convert to bytes for send_file
        bytes_buffer = BytesIO(text_buffer.getvalue().encode('utf-8'))
        bytes_buffer.seek(0)

        return send_file(
            bytes_buffer,
            mimetype='text/csv',
            as_attachment=True,
            download_name='audit_logs.csv'
        )
    except Exception as e:
        if conn:
            conn.close()
        flash(f'Error while generating audit CSV: {e}', 'danger')
        return redirect(url_for('reporting'))


@app.route('/get_customers_pending_tables')
@login_required
def get_customers_pending_tables():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT DISTINCT cm.customer_name
        FROM customer_mapping_table cm
        WHERE (cm.customer_name, cm.month_year) 
              NOT IN (SELECT customer_name, month_year FROM final_computed_table)
        ORDER BY cm.customer_name
    """)

    customers = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()

    return jsonify({"customers": customers})

@app.route('/get_months_pending_tables/<customer>')
@login_required
def get_months_pending_tables(customer):
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT cm.month_year
        FROM customer_mapping_table cm
        WHERE cm.customer_name = %s
          AND (cm.customer_name, cm.month_year)
              NOT IN (SELECT customer_name, month_year FROM final_computed_table)
        ORDER BY cm.month_year
    """, (customer,))

    months = [row[0].strftime("%Y-%m") for row in cur.fetchall()]

    cur.close()
    conn.close()

    return jsonify({"months": months})

@app.route('/delete_record', methods=['POST'])
@login_required
def delete_record():
    """
    Delete a record from the database with proper validation and error handling
    """
    import logging
    
    # Configure logger
    if not app.logger.handlers:
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        app.logger.addHandler(handler)
        app.logger.setLevel(logging.INFO)
    
    try:
        customer = request.form.get('customer') or ''
        month_raw = request.form.get('month') or ''
        customer = customer.strip()

        print(f"\n{'='*60}")
        print(f"[DELETE] Request received")
        print(f"[DELETE] Customer: {customer}")
        print(f"[DELETE] Month (raw): {month_raw}")
        print(f"{'='*60}\n")

        if not customer or not month_raw:
            msg = 'Customer and month are required.'
            print(f"[DELETE] ERROR: {msg}")
            return jsonify({'success': False, 'message': msg}), 400

        # --- Normalize month value robustly to a date (first day of month) ---
        try:
            m = (month_raw or "").strip()
            month_date = None

            # Case: already full date (YYYY-MM-DD or ISO datetime)
            for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
                try:
                    dt = datetime.strptime(m, fmt)
                    month_date = dt.date().replace(day=1)
                    break
                except Exception:
                    pass

            # Case: month-only like YYYY-MM
            if month_date is None and re.match(r'^\d{4}-\d{2}$', m):
                month_date = datetime.strptime(m + "-01", "%Y-%m-%d").date()

            # Case: some clients might send MM/DD/YYYY — try that (fallback)
            if month_date is None:
                try:
                    dt = datetime.strptime(m, "%m/%d/%Y")
                    month_date = dt.date().replace(day=1)
                except Exception:
                    pass

            if month_date is None:
                # Last attempt: try parsing as ISO using fromisoformat (Python 3.7+)
                try:
                    dt = datetime.fromisoformat(m)
                    month_date = dt.date().replace(day=1)
                except Exception:
                    month_date = None

            if month_date is None:
                raise ValueError(f"Unrecognized month format: '{month_raw}'")

        except Exception as e:
            msg = f'Invalid month format: {str(e)}'
            print(f"[DELETE] ERROR: {msg}")
            return jsonify({'success': False, 'message': msg}), 400
        # --- end normalization ---


        print(f"[DELETE] Normalized month: {month_date}")

        # Get DB connection with detailed error handling
        conn = get_db_connection()
        if not conn:
            print("[DELETE] CRITICAL: Database connection failed!")
            print(f"[DELETE] Session username: {session.get('username', 'NOT SET')}")
            print(f"[DELETE] DB Config: {DB_CONFIG}")
            return jsonify({'success': False, 'message': 'Database connection failed. Please check your session or re-login.'}), 500

        print("[DELETE] Database connection successful")

        try:
            cur = conn.cursor()

            # First, let's check what data exists for this customer
            print(f"\n[DELETE] Checking all records for customer '{customer}':")
            cur.execute("""
                SELECT customer_name, month_year::text 
                FROM final_computed_table 
                WHERE customer_name = %s
                ORDER BY month_year
            """, (customer,))
            all_records = cur.fetchall()
            print(f"[DELETE] Found {len(all_records)} total records for this customer:")
            for rec in all_records:
                print(f"  - {rec[0]}: {rec[1]}")

            # Check if the specific record exists
            check_query = """
                SELECT customer_name, month_year::text 
                FROM final_computed_table
                WHERE customer_name = %s AND month_year = %s
            """
            print(f"\n[DELETE] Checking existence with query:")
            print(f"  SQL: {check_query}")
            print(f"  Params: customer='{customer}', month='{month_date}'")
            
            cur.execute(check_query, (customer, month_date))
            found_record = cur.fetchone()
            
            if found_record:
                print(f"[DELETE] ✓ Record EXISTS: {found_record[0]} - {found_record[1]}")
            else:
                print(f"[DELETE] ✗ Record NOT FOUND")
                print(f"[DELETE] Looking for exact match: customer='{customer}', month='{month_date}'")
                
                # Try alternative formats
                print(f"\n[DELETE] Trying alternative date formats...")
                
                # Try YYYY-MM-DD
                cur.execute("""
                    SELECT customer_name, month_year::text 
                    FROM final_computed_table
                    WHERE customer_name = %s AND month_year::text = %s
                """, (customer, month_date))
                alt_result = cur.fetchone()
                
                if alt_result:
                    print(f"[DELETE] Found with string comparison: {alt_result}")
                else:
                    print(f"[DELETE] Still not found with string comparison")
                
                cur.close()
                conn.close()
                return jsonify({
                    'success': False, 
                    'message': f'Record for {customer} - {month_date} not found. Available dates: {[r[1] for r in all_records]}'
                }), 404

                        # If we reach here, record exists - proceed with deletion
            print(f"\n[DELETE] Proceeding with deletion.")
            
            deleted_counts = {}
            delete_statements = [
                ("final_computed_table", "DELETE FROM final_computed_table WHERE customer_name = %s AND month_year = %s"),
                ("availability_table", "DELETE FROM availability_table WHERE customer_name = %s AND month_year = %s"),
                ("users_table", "DELETE FROM users_table WHERE customer_name = %s AND month_year = %s"),
                ("storage_table", "DELETE FROM storage_table WHERE customer_name = %s AND month_year = %s"),
                ("tickets_computed_table", "DELETE FROM tickets_computed_table WHERE customer_name = %s AND month_year = %s"),
                ("customer_mapping_table", "DELETE FROM customer_mapping_table WHERE LOWER(TRIM(customer_name)) = LOWER(TRIM(%s)) AND month_year = %s"),

            ]

            for table_name, stmt in delete_statements:
                try:
                    print(f"[DELETE] Deleting from {table_name}.")
                    cur.execute(stmt, (customer, month_date))
                    rows_deleted = cur.rowcount if cur.rowcount is not None else 0
                    deleted_counts[table_name] = rows_deleted
                    print(f"[DELETE]   ✓ Deleted {rows_deleted} row(s) from {table_name}")
                except Exception as table_err:
                    print(f"[DELETE]   ✗ Error deleting from {table_name}: {table_err}")
                    deleted_counts[table_name] = f"Error: {str(table_err)}"

            # Commit the transaction
            conn.commit()
            print(f"\n[DELETE] ✓ Transaction committed successfully")
            
            cur.close()
            conn.close()

            print(f"[DELETE] Deletion summary: {deleted_counts}")
            print(f"{'='*60}\n")

            # --- NEW: ensure something was actually deleted ---
            total_deleted = 0
            for v in deleted_counts.values():
                try:
                    # Only count numeric deletions
                    total_deleted += int(v)
                except Exception:
                    # skip non-numeric (error strings)
                    pass

            if total_deleted <= 0:
                # No rows were removed (unexpected after existence check) — return explicit failure
                return jsonify({
                    'success': False,
                    'message': f'No rows were deleted for {customer} - {month_date}. Please verify the record exists.',
                    'deleted_counts': deleted_counts
                }), 404

            # Normal success response (at least one row removed)
            return jsonify({
                'success': True, 
                'message': f'Record for {customer} - {month_date} has been successfully deleted from {total_deleted} row(s).',
                'deleted_counts': deleted_counts
            }), 200


        except psycopg2.Error as db_err:
            print(f"\n[DELETE] ✗ Database error: {db_err}")
            print(f"[DELETE] Error type: {type(db_err).__name__}")
            import traceback
            traceback.print_exc()
            
            try:
                conn.rollback()
                cur.close()
                conn.close()
            except:
                pass
            
            return jsonify({'success': False, 'message': f'Database error: {str(db_err)}'}), 500

    except Exception as e:
        print(f"\n[DELETE] ✗ Unexpected error: {e}")
        print(f"[DELETE] Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': f'Server error: {str(e)}'}), 500


@app.route('/check_record_exists', methods=['POST'])
@login_required
def check_record_exists():
    """Check if a record already exists for a given customer and month."""
    try:
        customer = request.form.get('customer', '').strip()
        month_str = request.form.get('month', '').strip()
        
        if not customer or not month_str:
            return jsonify({'success': False, 'exists': False, 'message': 'Customer and month are required'})
        
        # Parse the month string to a date object
        try:
            month_date = datetime.strptime(month_str, '%Y-%m-%d').date()
        except ValueError:
            return jsonify({'success': False, 'exists': False, 'message': 'Invalid date format'})
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'success': False, 'exists': False, 'message': 'Database connection failed'})
        
        cur = conn.cursor()
        
        # Check if record exists in final_computed_table
        cur.execute("""
            SELECT COUNT(*) FROM final_computed_table 
            WHERE customer_name = %s AND month_year = %s
        """, (customer, month_date))
        
        count = cur.fetchone()[0]
        exists = count > 0
        
        cur.close()
        conn.close()
        
        return jsonify({'success': True, 'exists': exists})
        
    except Exception as e:
        print(f"Error in check_record_exists: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'exists': False, 'message': str(e)})

@app.route('/attach_comment', methods=['POST'])
@login_required
def attach_comment():
    """
    Attach user comment to the correct audit_logs record based on:
    customer_name, month_year/month, section, operation_type.
    Matches rows even if month stored with timestamps or different keys.
    """
    try:
        data = request.get_json()

        customer = data.get("customer")
        month    = data.get("month")           # string yyyy-mm-dd
        section  = data.get("section")         # availability/users/storage/tickets/config
        comment  = data.get("comment")
        op_type  = data.get("operation")       # UPDATE / INSERT / DELETE

        if not (customer and month and section and comment and op_type):
            return jsonify({
                "success": False,
                "message": "Missing required fields"
            }), 400

        # Normalize month into date
        try:
            month_date = datetime.strptime(month, "%Y-%m-%d").date()
        except:
            return jsonify({
                "success": False,
                "message": "Invalid month format"
            }), 400

        # Map section → table
        TABLE_MAP = {
            "availability": "availability_table",
            "users":        "users_table",
            "storage":      "storage_table",
            "tickets":      "tickets_computed_table",  # or tickets table if exists
            "config":       "customer_mapping_table"
        }

        table_name = TABLE_MAP.get(section.lower())
        if not table_name:
            return jsonify({"success": False, "message": "Invalid section"}), 400

        conn = get_db_connection()
        cur = conn.cursor()

        # 🟢 UPDATED — MATCH BOTH KEYS + TIMESTAMP FORMATS + ONLY NULL COMMENT
        sql_select = """
            SELECT audit_id
            FROM audit_logs
            WHERE 
                table_name = %s
                AND operation_type = %s
                AND primary_key_value->>'customer_name' = %s
                AND (
                        primary_key_value->>'month_year' LIKE %s
                     OR primary_key_value->>'month' LIKE %s
                )
                AND (comment IS NULL OR comment = '')
            ORDER BY changed_at DESC
            LIMIT 1;
        """

        month_pattern = f"{month_date}%"  # matches 2025-11-01, 2025-11-01 00:00:00, etc.

        cur.execute(sql_select, (
            table_name,
            op_type,
            customer,
            month_pattern,
            month_pattern
        ))

        row = cur.fetchone()

        if not row:
            cur.close()
            conn.close()
            return jsonify({
                "success": False,
                "message": (
                    f"No matching audit record found.\n"
                    f"customer={customer}, month={month}, section={section}, operation={op_type}"
                )
            }), 404

        audit_id = row[0]

        # 🟢 Finally update comment + section_name
        sql_update = """
            UPDATE audit_logs
            SET 
                comment = %s,
                section_name = %s
            WHERE audit_id = %s;
        """

        cur.execute(sql_update, (comment, section, audit_id))
        conn.commit()

        cur.close()
        conn.close()

        return jsonify({"success": True})

    except Exception as e:
        return jsonify({
            "success": False,
            "message": str(e)
        }), 500
    
@app.route("/get_months_for_csm", methods=["POST"])
@login_required
def get_months_for_csm():
    data = request.get_json()
    csm = data.get("csm")

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT DISTINCT TO_CHAR(date_trunc('month', month_year), 'YYYY-MM')
        FROM final_computed_table
        WHERE csm_primary = %s OR csm_secondary = %s
        ORDER BY 1;
    """, (csm, csm))

    months = [r[0] for r in cur.fetchall()]

    cur.close()
    conn.close()

    return jsonify({"success": True, "months": months})

@app.route("/load_multi_month_csm_data", methods=["POST"])
@login_required
def load_multi_month_csm_data():
    data = request.get_json()

    csm = data.get("csm")
    start_month = data.get("start_month")  # YYYY-MM-01
    num_months = int(data.get("num_months"))

    start_date = datetime.strptime(start_month, "%Y-%m-%d").date()

    # BACKWARD RANGE
    range_start = start_date - relativedelta(months=num_months - 1)
    range_end = start_date

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM final_computed_table
        WHERE (csm_primary = %s OR csm_secondary = %s)
          AND month_year BETWEEN %s AND %s
        ORDER BY customer_name, month_year DESC;
    """, (csm, csm, range_start, range_end))

    rows = cur.fetchall()
    cols = [c[0] for c in cur.description]
    json_rows = [dict(zip(cols, r)) for r in rows]

    cur.close()
    conn.close()

    return jsonify({"success": True, "data": json_rows})

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)
    #app.run(debug=True)