from pyramid.response import Response, FileIter, FileResponse
from pyramid.view import view_config
from utils import genbank
import tempfile
from models import Spacer, Job, Session

@view_config(route_name="gb_all_nicks")
def gb_all_nicks_view(request):
    tf = tempfile.NamedTemporaryFile(prefix='genbank_export_nick_%s' % request.job.key,
                                     suffix='.gb', delete=True)
    tf.write(genbank.all_nicks_to_GB(request.job.id))
    tf.seek(0)
    response = Response(content_type='text/plain')
    response.app_iter = tf
    response.headers['Content-Disposition'] = ("attachment; filename=all_nickases.gb")

    return response

@view_config(route_name="gb_all_guides")
def gb_all_guides_view(request):
    tf = tempfile.NamedTemporaryFile(prefix='genbank_export_guide_%s' % request.job.key,
                                     suffix='.gb', delete=True)
    tf.write(genbank.all_guides_to_GB(request.job.id))
    tf.seek(0)
    response = Response(content_type='text/plain')
    response.app_iter = tf
    response.headers['Content-Disposition'] = ("attachment; filename=all_guides.gb")

    return response

@view_config(route_name="gb_one_nick")
def gb_one_nick_view(request):
    #response = Response(content_type='application/csv')
    tf = tempfile.NamedTemporaryFile(prefix='genbank_export_one_nick_%s' % request.job.key,
                                     suffix='.gb', delete=True)
        # this is where I usually put stuff in the file


    job = request.job
    sf = Session.query(Spacer).get(request.matchdict["spacerfwdid"])
    sr = Session.query(Spacer).get(request.matchdict["spacerrevid"])

    tf.write(genbank.one_nick_to_GB(job,sf,sr))
    tf.seek(0)

    #response = Response(content_type='application/csv')
    response = Response(content_type='text/plain')
    response.app_iter = tf
    #response.headers['Content-Disposition'] = ("attachment; filename=nickase_export.gb")
    # a target=_blank
    return response




@view_config(route_name="csv_one_spacer")
def csv_one_spacer_view(request):
    tf = tempfile.NamedTemporaryFile(prefix='csv_export_one_spacer_%s' % request.job.key,
                                     suffix='.csv', delete=True)
        # this is where I usually put stuff in the file


    job = request.job
    spacer = Session.query(Spacer).get(request.matchdict["spacerid"])

    tf.write(genbank.one_spacer_to_CSV(job,spacer))
    tf.seek(0)

    response = Response(content_type='application/csv')
    response.app_iter = tf
    response.headers['Content-Disposition'] = ("attachment; filename=offtargets.csv")
    return response




@view_config(route_name="csv_all_guides")
def csv_all_guides_view(request):
    tf = tempfile.NamedTemporaryFile(prefix='csv_export_all_guides_%s' % request.job.key,
                                     suffix='.csv', delete=True)
        # this is where I usually put stuff in the file


    job = request.job

    tf.write(genbank.all_spacers_to_CSV(job))
    tf.seek(0)

    response = Response(content_type='application/csv')
    response.app_iter = tf
    response.headers['Content-Disposition'] = ("attachment; filename=offtargets.csv")
    return response

