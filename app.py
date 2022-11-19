from flask import Flask, send_file, request, abort
from flask_caching import Cache
from influenza_stat_parser import InfluenzaStatParser

app = Flask(__name__)
cache = Cache(app, config={'CACHE_TYPE': 'simple'})


@app.route("/influenza-stat/plot/")
@cache.cached(timeout=180, query_string=True)
def get_influenza_stat_plot():
    year = request.args.get('year', default=2022, type=int)
    start_week = request.args.get('start_week', default=1, type=int)
    end_week = request.args.get('end_week', default=52, type=int)
    parser = InfluenzaStatParser(year)
    parser.update_statistics_data()
    image_buf = parser.get_plot(start_week, end_week)
    if not image_buf:
        abort(404)
    image_buf.seek(0)
    return send_file(
        image_buf,
        mimetype='image/jpeg',
        as_attachment=True,
        download_name=f"influenza_stat_y{year}sw{start_week}ew{end_week}.jpeg")


if __name__ == '__main__':
    app.run(debug=True)
