from app import app
import flask
import datetime
import dash
import re
import logging
import string

import pandas as pd
import dash_html_components as html
import dash_core_components as dcc
import plotly.graph_objs as go
import plotly.figure_factory as ff

from flask import request, redirect, url_for, session, flash
from functools import wraps
from app.modules import search_elastic
from collections import Counter
from elasticsearch import Elasticsearch
from nltk.corpus import stopwords
from flask_paginate import Pagination, get_page_args

from flasgger import Swagger, swag_from

from app.modules import api_ner_tagger

es = Elasticsearch(
    [{'host': 'localhost', 'port': 9200}], timeout=30, max_retries=10, retry_on_timeout=True
)

es_logger = logging.getLogger('elasticsearch')
es_logger.setLevel(logging.CRITICAL)

with open('/data2/SmartPub/app/secret_key.txt', 'r') as key:
    app.secret_key = key.readline()


def login_required(f):
    @wraps(f)
    def wrap(*args, **kwargs):
        if 'logged_in' in session:
            return f(*args, **kwargs)
        else:
            flash('You need to login first.')
            return redirect(url_for('login', _external=True))
    return wrap


def token_stopword_filter(word: str):
    tr = str.maketrans("", "", string.punctuation)
    no_punctuation = word.translate(tr)
    clean = ''.join([i for i in no_punctuation if not i.isdigit()])
    filtered_word = ' '.join([t for t in clean.lower().split() if t not in stopwords.words('english')])
    filtered_word = re.sub(r'\[[^)]*\]', '', filtered_word)
    filtered_word = re.sub(u"[^\w\d'\s\-]+", '', filtered_word)
    return filtered_word


@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'GET':
        try:
            id_list, title_list, journal_list, year_list, authors_list = search_elastic.popular_papers()
            return flask.render_template("main.html",
                                         zipped_lists=zip(id_list, title_list, journal_list, year_list, authors_list))
        except Exception:
            return flask.render_template("test.html")
        
    if request.method == 'POST':
        text = request.form.get('search')
        return redirect(url_for('search', search=text))


def get_ids(id_list=[], offset=0, per_page=10):
    return id_list[offset: offset + per_page]


@app.route('/autocomplete', methods=['GET'])
def autocomplete():
    search_text = request.args.get('q')
    app.logger.debug(search_text)
    results = search_elastic.autocomplete_query(search_text)
    return flask.jsonify(matching_results=results)


@app.route('/search-result', methods=['GET', 'POST'])
def search():  
    if request.method == 'GET':
        page, per_page, offset = get_page_args(page_parameter='page',
                                               per_page_parameter='per_page')
        text = request.args['search']
        id_list, title_list, journal_list, year_list, authors_list = search_elastic.dosearch(text)
        method_popular_entities, dataset_popular_entities, upcoming_entities = search_elastic.popular_upcoming_entities(
            id_list)
        word_cloud = search_elastic.word_cloud_for_first_page(id_list, text)
        method_popular_entities = method_popular_entities[:7]
        dataset_popular_entities = dataset_popular_entities[:7]
        zipped_lists = list(zip(id_list, title_list, journal_list, year_list, authors_list))
        count = (Counter([int(y) for y in year_list]))
        count = sorted(count.items())
        overview_count = []
        overview_label = []
        for x in count:
            overview_count.append(x[1])
            overview_label.append(str(x[0]))

        total = len(zipped_lists)
        pagination_ids = get_ids(id_list=zipped_lists, offset=offset, per_page=per_page)
        pagination = Pagination(page=page, per_page=per_page, total=total, css_framework='bootstrap4')
        return flask.render_template("search-result.html",
                                     zipped_lists=pagination_ids, total=total,
                                     search_text=text, # word_cloud=word_cloud,
                                     method_popular_entities=method_popular_entities,
                                     dataset_popular_entities=dataset_popular_entities,
                                     overview_count=overview_count, overview_label=overview_label,
                                     pagination=pagination, per_page=per_page, page=page)
    if request.method == 'POST':
        text = request.form.get('search')
        return redirect(url_for('search', search=text))


@app.route('/entities', methods=['GET', 'POST'])
def entities():
    if request.method == 'POST' and 'searchent' in request.form:
        entity = request.form['searchent']
        popular = search_elastic.wordcloud_entity(entity)
        id_list, title_list, journal_list, year_list, authors_list = search_elastic.dosearch_entity(entity)
        return flask.render_template("entities.html", entity_name=entity, popular=popular,
                                     zipped_lists=zip(id_list, title_list, journal_list, year_list, authors_list))
    if request.method == 'POST' and 'search' in request.form:
        text = request.form.get('search')
        return redirect(url_for('search', search=text))


@app.route('/entities/dataset/<entity>', methods=['POST', 'GET'])
def entities_dataset(entity):
    if request.method == 'GET':
        popular = search_elastic.wordcloud_entity(entity)
        id_list, title_list, journal_list, year_list, authors_list = search_elastic.dosearch_entity(entity)
        return flask.render_template("entities.html", entity_name=entity, popular=popular,
                                     zipped_lists=zip(id_list, title_list, journal_list, year_list, authors_list))
    if request.method == 'POST' and 'searchent' in request.form:
        entity = request.form['searchent']
        popular = search_elastic.wordcloud_entity(entity)
        id_list, title_list, journal_list, year_list, authors_list = search_elastic.dosearch_entity(entity)
        return flask.render_template("entities.html", entity_name=entity, popular=popular,
                                     zipped_lists=zip(id_list, title_list, journal_list, year_list, authors_list))
    if request.method == 'POST' and 'search' in request.form:
        text = request.form.get('search')
        return redirect(url_for('search', search=text))


@app.route('/entities/method/<entity>', methods=['POST', 'GET'])
def entities_method(entity):  
    if request.method == 'GET':
        popular = search_elastic.wordcloud_entity(entity)
        id_list, title_list, journal_list, year_list, authors_list = search_elastic.dosearch_entity(entity)
        return flask.render_template("entities.html", entity_name=entity, popular=popular,
                                     zipped_lists=zip(id_list, title_list, journal_list, year_list, authors_list))
    if request.method == 'POST' and 'searchent' in request.form:
        entity = request.form['searchent']
        popular = search_elastic.wordcloud_entity(entity)
        id_list, title_list, journal_list, year_list, authors_list = search_elastic.dosearch_entity(entity)
        return flask.render_template("entities.html", entity_name=entity, popular=popular,
                                     zipped_lists=zip(id_list, title_list, journal_list, year_list, authors_list))
    if request.method == 'POST' and 'search' in request.form:
        text = request.form.get('search')
        return redirect(url_for('search', search=text))



@app.route('/author/<author_name>', methods=['GET', 'POST'])
def author(author_name):
    if request.method == 'GET':
        authors_list_processed = []
        id_list, title_list, journal_list, year_list, authors_list, wordcloud = search_elastic.search_by_author(author_name)
        text_string = wordcloud
        for aut in authors_list:
            for aa in aut:
                if aa not in authors_list_processed and aa != author_name:
                    authors_list_processed.append(aa)
        return flask.render_template("author.html",
                                     zipped_lists=zip(id_list, title_list, journal_list, year_list, authors_list),
                                     author_name=author_name, number_of_pubs=len(id_list),
                                     authors_list_processed=authors_list_processed, text_string=text_string)
    if request.method == 'POST':
        text = request.form.get('search')
        return redirect(url_for('search', search=text))


@app.route('/publication/<publication_id>', methods=['POST', 'GET'])
def publication(publication_id):
    if request.method == 'GET':
        id_list, title_list, journal_list, year_list, authors_list, abstract_list, method_entities, dataset_entities, \
        amb_entities = search_elastic.search_by_id(publication_id)
        arxiv_id = publication_id.split('_')[1]
        abstract_list = ' '.join(abstract_list)
        paper_url = "https://arxiv.org/pdf/" + arxiv_id + ".pdf"
        return flask.render_template("publication.html",
                                     zipped_lists=zip(id_list, title_list, journal_list, year_list, authors_list),
                                     abstract=abstract_list, method=method_entities, dataset=dataset_entities,
                                     url=paper_url)
    if request.method == 'POST':
        text = request.form.get('search')
        return redirect(url_for('search', search=text))


@app.route('/entity_search', methods=['GET', 'POST'])
@login_required
def find_entity():  
    if request.method == 'GET':
        return flask.render_template("entity_to_update.html")
    if request.method == 'POST':
        entity = request.form['entity']
        clean_entity = token_stopword_filter(entity)
        if clean_entity:
            return redirect(url_for('.select_label', entity=clean_entity, _external=True))


@app.route('/godmode', methods=['GET', 'POST'])
@login_required
def select_label():
    if request.method == 'GET':
        entity = request.args['entity']
        info = search_elastic.get_entity_info(entity)
        count = len(info)
        return flask.render_template("godmode.html", entity=entity, count=count)
    if request.method == 'POST':
        label = request.form.get('optradio')
        entity = request.form.get('ent')
        if label and entity:
            search_elastic.update_db_godmode(entity, label)
            flash('Entity updated')
        else:
            flash('Entity not updated')
        return redirect(url_for('find_entity', _external=True))


@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    with open('/data2/SmartPub/app/credentials.txt', 'r') as cred_file:
        credentials = cred_file.readlines()
    credentials = [l.strip() for l in credentials]
    if request.method == 'POST':
        if request.form['username'] != credentials[0] or request.form['password'] != credentials[1]:
            error = 'Invalid Credentials. Please try again.'
        else:
            session['logged_in'] = True
            return flask.redirect(url_for('find_entity', _external=True))
    return flask.render_template('login.html', error=error)


@app.route('/logout')
@login_required
def logout():
    session.pop('logged_in', None)
    flash('You were logged out.')
    return redirect(url_for('login', _external=True))


@app.route('/my/data/<entity>')
def get_d3_data(entity):
    df = search_elastic.search_by_entity(entity)
    return df.to_csv()


@app.route('/my/updatedata/<entity>')
def get_d3_updatedata(entity):
    df = search_elastic.filter_by_conf(entity)
    return df.to_csv()


@app.route('/my/piedata/<entity>')
def get_d3_piedata(entity):
    df = search_elastic.filter_by_pie(entity)  
    return df.to_csv()


@app.route('/crowdsourcing/<entity>', methods=['GET'])
def crowdsourcing(entity):  
    sentences = search_elastic.select_sentence(entity)
    return flask.render_template("crowdsourcing.html", sentences=sentences, entity=entity)


@app.route('/crowdsourcing', methods=['GET', 'POST'])
def crowdsourcing_ambigious():
    entity = search_elastic.entities_for_crowdsourcing()

    if request.method == 'GET':
        sentences = search_elastic.select_sentence(entity)
        return flask.render_template("crowdsourcing.html", sentences=sentences, entity=entity)

    if request.method == 'POST':
        name1 = request.form.get('optradio')
        if name1:
            search_elastic.update_db_crowdsourcing(entity.lower(), name1)

        return redirect(url_for('crowdsourcing_ambigious'))


@app.route('/annotations', methods=['GET', 'POST'])
def annotation_ambigious():  # after pressing the search button
    sentences, post_id = search_elastic.select_reddit_post()
    if request.method == 'GET':
        return flask.render_template("annotations.html", sentences=sentences)
    if request.method == 'POST':
        try:
            name1 = request.form['seltext']
            if name1:
                search_elastic.add_annotation(post_id, name1)
        except:
            pass
        try:
            next_button = request.form["next_button"]
            if next_button:
                search_elastic.remove_temp(post_id)
                sentences, post_id = search_elastic.select_reddit_post()
        except:
            pass
        return flask.render_template("annotations.html", sentences=sentences)


dashboard = dash.Dash(__name__, server=app, url_base_pathname='/dashapp/', csrf_protect=False)
clean_stats = pd.read_pickle('/data2/SmartPub/app/modules/stats_pickles/clean_stats.pkl')
entity_stats = pd.read_pickle('/data2/SmartPub/app/modules/stats_pickles/entity_stats.pkl')
pub_stats = pd.read_pickle('/data2/SmartPub/app/modules/stats_pickles/pub_stats.pkl')

# dashboard = dash.Dash(__name__, server=app, url_base_pathname='/dashapp/')
# clean_stats = pd.read_pickle('app/modules/stats_pickles/clean_stats.pkl')
# entity_stats = pd.read_pickle('app/modules/stats_pickles/entity_stats.pkl')
# pub_stats = pd.read_pickle('app/modules/stats_pickles/pub_stats.pkl')

summary = clean_stats.groupby(['label', 'annotation'])['word'].count()
summary = summary.reset_index()
summary.columns = ['NER Label', 'Human Annotation', 'Count']

with open('/data2/SmartPub/app/modules/stats_pickles/update_log.txt', 'r') as f:
    dates = f.readlines()

total_pub = len(pub_stats)
total_ds = len(entity_stats.loc[(entity_stats['label'] == 'dataset')])
unique_ds = len(entity_stats.loc[(entity_stats['label'] == 'dataset')].groupby('word').sum())
total_mt = len(entity_stats.loc[(entity_stats['label'] == 'method')])
unique_mt = len(entity_stats.loc[(entity_stats['label'] == 'method')].groupby('word').sum())
total_ann = len(entity_stats.loc[(entity_stats['annotation'] != 'undefined')].groupby('word').sum())
last_update = dates[-1]
last_update = last_update[:10]
last = datetime.datetime.strptime(last_update, '%Y-%m-%d')
week = datetime.timedelta(days=7)
next_update = last + week

totals = [['', 'Values'],
          ['Total Publications', total_pub],
          ['Total Dataset Entities', total_ds],
          ['Unique Dataset Entities', unique_ds],
          ['Total Method Entities', total_mt],
          ['Unique Method Entities', unique_mt],
          ['Total Human Annotated Entities', total_ann],
          ['Last Update', last_update],
          ['Next Update', next_update]]

ds_ds = len(entity_stats.loc[(entity_stats['label'] == 'dataset') & (entity_stats['annotation'] == 'dataset')].groupby(
    'word').sum())
ds_na = len(
    entity_stats.loc[(entity_stats['label'] == 'dataset') & (entity_stats['annotation'] == 'undefined')].groupby(
        'word').sum())
ds_total = len(entity_stats.loc[(entity_stats['annotation'] == 'dataset')].groupby('word').sum())
mt_total = len(entity_stats.loc[(entity_stats['annotation'] == 'method')].groupby('word').sum())
mt_mt = len(entity_stats.loc[(entity_stats['label'] == 'method') & (entity_stats['annotation'] == 'dataset')].groupby(
    'word').sum())
mt_na = len(entity_stats.loc[(entity_stats['label'] == 'method') & (entity_stats['annotation'] == 'undefined')].groupby(
    'word').sum())
noise = len(entity_stats.loc[(entity_stats['annotation'] == 'noise')].groupby('word').sum())
other = len(entity_stats.loc[(entity_stats['annotation'].isin(['other', 'system']))].groupby('word').sum())

summ = [['Annotation', 'NER Label', 'Count'],
        ['Dataset', 'Dataset', ds_ds],
        ['Dataset Total', '', ds_total],
        ['Not Annotated', 'Dataset', ds_na],
        ['Method', 'Method', mt_mt],
        ['Method Total', '', mt_total],
        ['Not Annotated', 'Method', mt_na],
        ['Noise', '', noise],
        ['Other', '', other], ]

pie = pub_stats.groupby('keywords')['id'].count()
pie = pie.reset_index()
pie.columns = ['Domain', 'Count']

dashboard.css.append_css({"external_url": "https://codepen.io/chriddyp/pen/bWLwgP.css"})
dashboard.layout = html.Div(children=[
    html.H1(children='SmartPub Dashboard'),
    html.Div(children='''
        Main stats of publications and entities in the SmartPub system
    '''),
    html.Div(
        dcc.Graph(
            id='summary',
            figure=go.Figure(
                ff.create_table(totals, colorscale='Blues', index=True, height_constant=30)
            ),
            style={'width': '425'}
        ),
        style={'display': 'inline-block'}
    ),
    html.Div(
        dcc.Graph(
            id='spacer',
            figure=go.Figure(
                ff.create_table([['']], colorscale=[[0, '#ffffff'], [0.5, '#ffffff'], [1, '#ffffff']],
                                height_constant=100)
            ),
            style={'width': '100'}
        ),
        style={'display': 'inline-block'}
    ),
    html.Div(
        dcc.Graph(
            id='summary2',
            figure=go.Figure(
                ff.create_table(summ, colorscale='Blues', height_constant=30)
            ),
            style={'width': '600'}
        ),
        style={'display': 'inline-block'}
    ),
    html.Div(
        dcc.Graph(
            id='spacer2',
            figure=go.Figure(
                ff.create_table([['']], colorscale=[[0, '#ffffff'], [0.5, '#ffffff'], [1, '#ffffff']],
                                height_constant=100)
            ),
            style={'width': '50'}
        ),
        style={'display': 'inline-block'}
    ),
    html.Div(
        dcc.Graph(
            id='topic_pie',
            figure=go.Figure(
                data=[
                    go.Pie(
                        labels=pie['Domain'],
                        values=pie['Count'],
                        textinfo='none'
                    )
                ],
                layout=go.Layout(
                    title='Primary Topic of Publications',
                    showlegend=False,
#                     margin=go.layout.Margin(
                    margin=go.Margin(
                        l=5,
                        r=5,
                        b=10,
                        t=25,
                        pad=4
                    )
                )
            ),
            style={'height': '400'}
        ),
        style={'display': 'inline-block'}
    ),
    dcc.Graph(
        id='pub_ent',
        style={'height': 600},
        figure=go.Figure(
            data=[
                go.Bar(
                    x=clean_stats.loc[(clean_stats['label'] == 'method')].groupby('year')['word'].count().index,
                    y=clean_stats.loc[(clean_stats['label'] == 'method')].groupby('year')['word'].count(),
                    name='Method Entities'
                ),
                go.Bar(
                    x=clean_stats.loc[(clean_stats['label'] == 'dataset')].groupby('year')['word'].count().index,
                    y=clean_stats.loc[(clean_stats['label'] == 'dataset')].groupby('year')['word'].count(),
                    name='Dataset Entities'
                ),
                go.Scatter(
                    x=pub_stats.groupby('year')['id'].count().index,
                    y=pub_stats.groupby('year')['id'].count(),
                    name='Publications',
                    yaxis='y2'
                )
            ],
            layout=go.Layout(
                title='Entities & Publications per Year',
                showlegend=True,
#                 legend=go.layout.Legend(
                legend=go.Legend(
                    x=0,
                    y=1.0
                ),
                yaxis=dict(
                    title='Number of Entites'
                ),
                yaxis2=dict(
                    title='Number of Publications',
                    titlefont=dict(
                        color='rgb(148, 103, 189)'
                    ),
                    tickfont=dict(
                        color='rgb(148, 103, 189)'
                    ),
                    overlaying='y',
                    side='right'
                ),
                xaxis=dict(
                    ticks='outside',
                    tick0=0,
                    dtick=1,
                    ticklen=10,
                    tickwidth=4
                )
            )
        )
    )
])

Swagger(app)


@app.route('/api/<string:arxiv_id>/', methods=['GET'])
@swag_from('api_index.yml')
def api_index(arxiv_id):

    arxiv_id = arxiv_id.lower().strip()
    facet = request.args.get('facet')
    facet = facet.lower()
    id_list, title_list, journal_list, year_list, authors_list, abstract_list, \
    method_entities, dataset_entities, all_amb = search_elastic.search_by_id(arxiv_id)
    if facet not in ['both', 'dataset', 'method']:
        return "An error occurred, invalid facet requested. Try 'dataset', 'method' or 'both'", 500
    if facet == 'method':
        entity_list = method_entities
    if facet == 'dataset':
        entity_list = dataset_entities
    if facet == 'both':
        entity_list = all_amb
    return flask.jsonify(
        arxiv_id=arxiv_id,
        entities=entity_list
    )


@app.route('/api/entities_by_list/<string:arxiv_id_list>/', methods=['GET'])
@swag_from('api_index_list.yml')
def api_index_list(arxiv_id_list):

    arxiv_id_list = [arxiv_id.lower().strip() for arxiv_id in arxiv_id_list.split(',')]
    print(arxiv_id_list)
    facet = request.args.get('facet')
    facet = facet.lower()
    final_list = {}
    for arxiv_id in arxiv_id_list:
        id_list, title_list, journal_list, year_list, authors_list, abstract_list, \
        method_entities, dataset_entities, all_amb = search_elastic.search_by_id(arxiv_id)
        if facet not in ['both', 'dataset', 'method']:
            return "An error occurred, invalid facet requested. Try 'dataset', 'method' or 'both'", 500
        if facet == 'method':
            entity_list = method_entities
        if facet == 'dataset':
            entity_list = dataset_entities
        if facet == 'both':
            entity_list = all_amb
        final_list[arxiv_id] = entity_list

    return flask.jsonify(
        arxiv_id=arxiv_id_list,
        entities=final_list
    )

@app.route('/api/entities_in_text/<string:text_block>/', methods=['GET'])
@swag_from('api_index_text.yml')
def api_index_text(text_block):
    facet = request.args.get('facet')
    facet = facet.lower().strip()
    print(len(text_block))

    final_list = {}
    if facet == 'both':
        for facet in ['MET', 'DATA']:
            entity_list = api_ner_tagger.tag_text_block(text_block, facet)
            final_list[facet] = entity_list
            print(entity_list)

    elif facet == 'method':
        facet = 'MET'
        final_list[facet] = api_ner_tagger.tag_text_block(text_block, facet)

    elif facet == 'dataset':
        facet = 'DATA'
        final_list[facet] = api_ner_tagger.tag_text_block(text_block, facet)

    return flask.jsonify(
        text=text_block,
        entities=final_list
    )

