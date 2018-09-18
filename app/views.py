from app import app
import flask
import datetime
from flask import Flask, request, render_template, redirect, url_for
from app.modules import search_elastic
import pandas as pd
from collections import Counter

import dash
import dash_html_components as html
import dash_core_components as dcc
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import plotly.figure_factory as ff

from elasticsearch import Elasticsearch
from elasticsearch import helpers

es = Elasticsearch(
    [{'host': 'localhost', 'port': 9200}], timeout=30, max_retries=10, retry_on_timeout=True
)

# @app.route("/")
# def hello():
#     return "<h1 style='color:blue'>Welcome to SmartPub: Search and Compare Scientific Publications</h1>"


@app.route('/')
@app.route('/')
def index():
    return flask.render_template("main.html")


@app.route('/search-result', methods=['POST'])
def search():  # after pressing the search button
    text = request.form['search']
    id_list, title_list, journal_list, year_list, authors_list = search_elastic.dosearch(text)
    method_popular_entities, dataset_popular_entities, upcoming_entities = search_elastic.popular_upcoming_entities(
        id_list)
    word_cloud = search_elastic.word_cloud_for_first_page(id_list, text)

    method_popular_entities = method_popular_entities[:7]
    dataset_popular_entities = dataset_popular_entities[:7]
    
    count = (Counter(year_list))
    count = sorted(count.items())
    overview_count = []
    overview_label = []
    for x in count:
        overview_count.append(x[1])
        overview_label.append(str(x[0]))
    
    return flask.render_template("search-result.html",
                                 zipped_lists=zip(id_list, title_list, journal_list, year_list, authors_list),
                                 search_text=text, word_cloud=word_cloud,
                                 method_popular_entities=method_popular_entities,
                                 dataset_popular_entities=dataset_popular_entities,
                                 overview_count=overview_count, overview_label=overview_label)


@app.route('/search-result', methods=['GET'])
def search_post():  # after pressing the search button
    q = request.args.get('q')
    if q:
        search = True
    try:
        page = int(request.args.get('page', 1))
    except ValueError:
        page = 1

    return flask.render_template("search-result.html")


@app.route('/entities', methods=['GET', 'POST'])
def entities():  # after pressing the search button
    entity = request.form['searchent']

    popular = search_elastic.wordcloud_entity(entity)

    id_list, title_list, journal_list, year_list, authors_list = search_elastic.dosearch_entity(entity)
#     print(title_list)

    return flask.render_template("entities.html", entity_name=entity, popular=popular,
                                 zipped_lists=zip(id_list, title_list, journal_list, year_list, authors_list))


@app.route('/publication/<publication_id>', methods=['POST', 'GET'])
def publication(publication_id):
    if request.method == 'POST':
        text = request.form['search']
        id_list, title_list, journal_list, year_list, authors_list, method_entities, dataset_entities, amb_entities = search_elastic.search_by_id(
            text)

        return flask.render_template("search.html",
                                     zipped_lists=zip(id_list, title_list, journal_list, year_list), title=title_list,
                                     journal=journal_list, year=year_list, authors=authors_list)

    else:
        publicationid = publication_id
        id_list, title_list, journal_list, year_list, authors_list, abstract_list, method_entities, dataset_entities, amb_entities = search_elastic.search_by_id(
            publicationid)
        arxiv_id = publication_id.split('_')[1]
        abstract_list = ' '.join(abstract_list)
        paper_url = "https://arxiv.org/pdf/" + arxiv_id + ".pdf"
        return flask.render_template("publication.html",
                                     zipped_lists=zip(id_list, title_list, journal_list, year_list, authors_list),
                                     abstract=abstract_list, method=method_entities, dataset=dataset_entities, url=paper_url)


@app.route('/author/<author_name>')
def author(author_name):
    # publicationid = publication_id
    authors_list_processed = []
    id_list, title_list, journal_list, year_list, authors_list, wordcloud = search_elastic.search_by_author(author_name)
    print(authors_list)
    # authors_list_processed=list(set(authors_list))
    # authors_list_processed.remove(author_name)
    text_string = wordcloud
    for aut in authors_list:
        for aa in aut:
            if aa not in authors_list_processed and aa != author_name:
                authors_list_processed.append(aa)

    return flask.render_template("author.html",
                                 zipped_lists=zip(id_list, title_list, journal_list, year_list, authors_list),
                                 author_name=author_name, number_of_pubs=len(id_list),
                                 authors_list_processed=authors_list_processed, text_string=text_string)


@app.route('/entities/dataset/<entity>', methods=['POST', 'GET'])
def entities_dataset(entity):  # after pressing the search button

    popular = search_elastic.wordcloud_entity(entity)
    id_list, title_list, journal_list, year_list, authors_list = search_elastic.dosearch_entity(entity)

    return flask.render_template("entities.html", entity_name=entity, popular=popular,
                                 zipped_lists=zip(id_list, title_list, journal_list, year_list, authors_list))


@app.route('/entities/method/<entity>', methods=['POST', 'GET'])
def entities_method(entity):  # after pressing the search button
    popular = search_elastic.wordcloud_entity(entity)

    id_list, title_list, journal_list, year_list, authors_list = search_elastic.dosearch_entity(entity)
    print(title_list)

    return flask.render_template("entities.html", entity_name=entity, popular=popular,
                                 zipped_lists=zip(id_list, title_list, journal_list, year_list, authors_list))


@app.route('/author/search', methods=['POST', 'GET'])
def search_inpub_authors():  # after pressing the search button
    text = request.form['search']
    id_list, title_list, journal_list, year_list, authors_list = search_elastic.dosearch(text)

    return flask.render_template("search.html",
                                 zipped_lists=zip(id_list, title_list, journal_list, year_list, authors_list),
                                 search_text=text)


@app.route('/publication/search', methods=['POST', 'GET'])
def search_inpub_publications():  # after pressing the search button
    text = request.form['search']
    id_list, title_list, journal_list, year_list, authors_list = search_elastic.dosearch(text)

    return flask.render_template("search.html",
                                 zipped_lists=zip(id_list, title_list, journal_list, year_list, authors_list),
                                 search_text=text)


@app.route('/my/data/<entity>')
def get_d3_data(entity):
    df = search_elastic.search_by_entity(entity)  # Constructed however you need it
    return df.to_csv()


@app.route('/my/updatedata/<entity>')
def get_d3_updatedata(entity):
    print(entity)
    df = search_elastic.filter_by_conf(entity)  # Constructed however you need it
    return df.to_csv()


@app.route('/my/piedata/<entity>')
def get_d3_piedata(entity):
    print(entity)
    df = search_elastic.filter_by_pie(entity)  # Constructed however you need it
    print(df)
    return df.to_csv()

@app.route('/test')
def hello_world():
    return flask.render_template("annotations.html", sentences=sentences)

@app.route('/crowdsourcing/<entity>', methods=['GET'])
def crowdsourcing(entity):  # after pressing the search button
    sentences = search_elastic.select_sentence(entity)
    return flask.render_template("crowdsourcing.html", sentences=sentences, entity=entity)


@app.route('/crowdsourcing', methods=['GET', 'POST'])
def crowdsourcing_ambigious():  # after pressing the search button
    entity = search_elastic.entities_for_crowdsourcing()

    if request.method == 'GET':
        sentences = search_elastic.select_sentence(entity)
        return flask.render_template("crowdsourcing.html", sentences=sentences, entity=entity)

    if request.method == 'POST':
        name1 = request.form.get('optradio')
        if name1:
            search_elastic.update_db_crowdsourcing(entity.lower(), name1)

        return redirect(url_for('crowdsourcing_ambigious'))
    
@app.route('/annotations', methods=['GET','POST'])
def annotation_ambigious():  # after pressing the search button
    sentences, post_id = search_elastic.select_reddit_post()

    if request.method == 'GET':
        return flask.render_template("annotations.html", sentences=sentences)
#         return flask.render_template("annotations.html", sentences='this is a test sentence')

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



        #return redirect(url_for('annotation_ambigious'))
        return flask.render_template("annotations.html", sentences=sentences)



# @app.route('/dashboard/')
# def about():
#     publications_stats, entities_stats = search_elastic.dashboard()
#     return render_template('dashboard.html', publications_stats=publications_stats, entities_stats=entities_stats)


dashboard = dash.Dash(__name__, server=app, url_base_pathname='/dashapp')

clean_stats = pd.read_pickle('/data2/SmartPub/app/modules/stats_pickles/clean_stats.pkl')
entity_stats = pd.read_pickle('/data2/SmartPub/app/modules/stats_pickles/entity_stats.pkl')
pub_stats = pd.read_pickle('/data2/SmartPub/app/modules/stats_pickles/pub_stats.pkl')

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

ds_ds = len(entity_stats.loc[(entity_stats['label'] == 'dataset') & (entity_stats['annotation'] == 'dataset')].groupby('word').sum())
ds_na = len(entity_stats.loc[(entity_stats['label'] == 'dataset') & (entity_stats['annotation'] == 'undefined')].groupby('word').sum())
ds_total = len(entity_stats.loc[(entity_stats['annotation'] == 'dataset')].groupby('word').sum())
mt_total = len(entity_stats.loc[(entity_stats['annotation'] == 'method')].groupby('word').sum())
mt_mt = len(entity_stats.loc[(entity_stats['label'] == 'method') & (entity_stats['annotation'] == 'dataset')].groupby('word').sum())
mt_na = len(entity_stats.loc[(entity_stats['label'] == 'method') & (entity_stats['annotation'] == 'undefined')].groupby('word').sum())
noise = len(entity_stats.loc[(entity_stats['annotation'] == 'noise')].groupby('word').sum())
other = len(entity_stats.loc[(entity_stats['annotation'].isin(['other', 'system']))].groupby('word').sum())

summ = [['Annotation', 'NER Label', 'Count'],
        ['Dataset', 'Dataset', ds_ds],
        ['Dataset Total', '', ds_total],
        ['Not Annotated', 'Dataset',  ds_na],
        ['Method', 'Method', mt_mt],
        ['Method Total', '', mt_total],
        ['Not Annotated', 'Method', mt_na],
        ['Noise', '', noise],
        ['Other', '', other],]

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
                ff.create_table([['']], colorscale = [[0, '#ffffff'], [0.5, '#ffffff'], [1, '#ffffff']], height_constant=100)
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
                ff.create_table([['']], colorscale = [[0, '#ffffff'], [0.5, '#ffffff'], [1, '#ffffff']], height_constant=100)
            ),
        style={'width': '50'}
        ),
        style={'display': 'inline-block'}
    ),
    html.Div(
        dcc.Graph(
            id='topic_pie',
            figure = go.Figure(
                data = [
                    go.Pie(
                        labels=pie['Domain'], 
                        values=pie['Count'],
                        textinfo='none'
                    )
                ],
                layout = go.Layout(
                    title = 'Primary Topic of Publications',
                    showlegend = False,
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
                    autotick=False,
                    ticks='outside',
                    tick0=0,
                    dtick=1,
                    ticklen=10,
                    tickwidth=4
                )
            )
        )
    ),
#     dcc.Graph(
#         style={'height': 300},
#         id='Plastic-Example',
#         figure=go.Figure(
#             data=[
#                 go.Bar(
#                     x=[1995, 1996, 1997, 1998, 1999, 2000, 2001, 2002, 2003,
#                        2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012],
#                     y=[219, 146, 112, 127, 124, 180, 236, 207, 236, 263,
#                        350, 430, 474, 526, 488, 537, 500, 439],
#                     name='Rest of world',
#                     marker=go.Marker(
#                         color='rgb(55, 83, 109)'
#                     )
#                 ),
#                 go.Bar(
#                     x=[1995, 1996, 1997, 1998, 1999, 2000, 2001, 2002, 2003,
#                        2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012],
#                     y=[16, 13, 10, 11, 28, 37, 43, 55, 56, 88, 105, 156, 270,
#                        299, 340, 403, 549, 499],
#                     name='China',
#                     marker=go.Marker(
#                         color='rgb(26, 118, 255)'
#                     )
#                 )
#             ],
#             layout=go.Layout(
#                 title='US Export of Plastic Scrap',
#                 showlegend=True,
#                 legend=go.Legend(
#                     x=0,
#                     y=1.0
#                 ),
#                 margin=go.Margin(l=40, r=0, t=40, b=30)
#             )
#         )
#     )
])
