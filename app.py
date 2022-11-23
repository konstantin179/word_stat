import os
from flask import Flask, send_file, request, abort
from flask_caching import Cache
from influenza_stat_parser import InfluenzaStatParser
from yandex_wordstat_api import get_ya_word_stat_plot
from google_trends_api import get_google_trends_plot_by_week, get_google_trends_plot_by_month
from postgres import DB
from dotenv import load_dotenv


app = Flask(__name__)
cache = Cache(app, config={'CACHE_TYPE': 'simple'})


@app.route("/phrases/", methods=['GET', 'POST'])
def get_phrases():
    """Returns saved phrases from db.
    Saved given phrases in db if POST method with
    json: {"phrases": [str, ]}."""
    load_dotenv()
    conn_string = os.getenv("DB_CONN_STR")
    phrases = []
    if request.method == 'POST':
        data = request.get_json()
        phrases = data['phrases']
        phrases = [{"phrase": phrase} for phrase in phrases]
        if conn_string:
            with DB(conn_string) as db:
                db.insert_values_into_phrases_table(phrases)
                db.delete_duplicates_from_phrases_table()
    if conn_string:
        with DB(conn_string) as db:
            phrases = db.get_phrases()
    return {"saved_phrases": phrases}


@app.route("/influenza-stat/week/plot/")
@cache.cached(timeout=180, query_string=True)
def get_influenza_stat_plot_by_week():
    """Returns image with graph of influenza statistics by week,
        starting from week number start_week to week number end_week.
    parameters:
    - name: year
        in: path
        type: int
    - name: start_week
        in: path
        type: int
    - name: end_week
        in: path
        type: int"""
    year = request.args.get('year', default=2022, type=int)
    start_week = request.args.get('start_week', default=1, type=int)
    end_week = request.args.get('end_week', default=52, type=int)
    parser = InfluenzaStatParser(year)
    parser.update_statistics_data()
    image_buf = parser.get_plot_by_week(start_week, end_week)
    if not image_buf:
        abort(404)
    image_buf.seek(0)
    return send_file(
        image_buf,
        mimetype='image/jpeg',
        as_attachment=True,
        download_name=f"influenza_stat_y{year}w{start_week}-{end_week}.jpeg")


@app.route("/influenza-stat/month/plot/")
@cache.cached(timeout=180, query_string=True)
def get_influenza_stat_plot_by_month():
    """Returns image with graph of influenza statistics by month,
        starting from start_month to end_month.
    parameters:
    - name: year
        in: path
        type: int
    - name: start_month
        in: path
        type: int
    - name: end_month
        in: path
        type: int"""
    year = request.args.get('year', default=2022, type=int)
    start_month = request.args.get('start_month', default=1, type=int)
    end_month = request.args.get('end_month', default=12, type=int)
    parser = InfluenzaStatParser(year)
    parser.update_statistics_data()
    image_buf = parser.get_plot_by_month(start_month, end_month)
    if not image_buf:
        abort(404)
    image_buf.seek(0)
    return send_file(
        image_buf,
        mimetype='image/jpeg',
        as_attachment=True,
        download_name=f"influenza_stat_y{year}m{start_month}-{end_month}.jpeg")


@app.route("/yandex-wordstat/plot/")
@cache.cached(timeout=180, query_string=True)
def get_yandex_word_stat_plot():
    """Returns image with graph of phrase yandex search statistics by month,
    starting from start_month to end_month.
    parameters:
    - name: phrase
        in: path
        type: string
        required: true
    - name: year
        in: path
        type: int
    - name: start_month
        in: path
        type: int
    - name: end_month
        in: path
        type: int"""
    phrase = request.args.get('phrase', type=str)
    year = request.args.get('year', default=2022, type=int)
    start_month = request.args.get('start_month', default=1, type=int)
    end_month = request.args.get('end_month', default=12, type=int)
    image_buf = get_ya_word_stat_plot(phrase, year, start_month, end_month)
    if not image_buf:
        abort(404)
    image_buf.seek(0)
    return send_file(
        image_buf,
        mimetype='image/jpeg',
        as_attachment=True,
        download_name=f"yws_{phrase}_y{year}m{start_month}-{end_month}.jpeg")


@app.route("/googletrends-stat/week/plot/")
@cache.cached(timeout=180, query_string=True)
def get_google_trends_stat_plot_by_week():
    """Returns image with graph of phrase google search statistics by week,
            starting from week number start_week to week number end_week.
    parameters:
    - name: phrase
        in: path
        type: string
        required: true
    - name: year
        in: path
        type: int
    - name: start_week
        in: path
        type: int
    - name: end_week
        in: path
        type: int"""
    phrase = request.args.get('phrase', type=str)
    year = request.args.get('year', default=2022, type=int)
    start_week = request.args.get('start_week', default=1, type=int)
    end_week = request.args.get('end_week', default=52, type=int)
    image_buf = get_google_trends_plot_by_week(phrase, year, start_week, end_week)
    if not image_buf:
        abort(404)
    image_buf.seek(0)
    return send_file(
        image_buf,
        mimetype='image/jpeg',
        as_attachment=True,
        download_name=f"gt_{phrase}_y{year}w{start_week}-{end_week}.jpeg")


@app.route("/googletrends-stat/month/plot/")
@cache.cached(timeout=180, query_string=True)
def get_google_trends_stat_plot_by_month():
    """Returns image with graph of phrase google search statistics by month,
    starting from start_month to end_month.
    parameters:
    - name: phrase
        in: path
        type: string
        required: true
    - name: year
        in: path
        type: int
    - name: start_month
        in: path
        type: int
    - name: end_month
        in: path
        type: int"""
    phrase = request.args.get('phrase', type=str)
    year = request.args.get('year', default=2022, type=int)
    start_month = request.args.get('start_month', default=1, type=int)
    end_month = request.args.get('end_month', default=12, type=int)
    image_buf = get_google_trends_plot_by_month(phrase, year, start_month, end_month)
    if not image_buf:
        abort(404)
    image_buf.seek(0)
    return send_file(
        image_buf,
        mimetype='image/jpeg',
        as_attachment=True,
        download_name=f"gt_{phrase}_y{year}m{start_month}-{end_month}.jpeg")


if __name__ == '__main__':
    app.run(debug=True)
