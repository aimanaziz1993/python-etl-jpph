try:
    # python lib
    from time import time
    import json
    
    # SQLAlchemy lib
    from sqlalchemy.exc import SQLAlchemyError

    # local
    import settings
    from engine import Engine, InitializeEngine
    from process import process_tv_trx_lot, process_tv_trx_lot_newt
    from utils import util_log
    log = util_log.logger()
except ImportError:
    raise Exception("import util files failed")

start = time()
res = []

try:
    engine = Engine(settings.GOMPB_DICT)
    initialize = InitializeEngine()
    gompb_engine = initialize.create(engine)
    gompb_engine.connect()

    if gompb_engine.connect():
        print('gompb engine creation...', gompb_engine)

        for p in settings.processes:
            lower_name = p.lower()
            # print(lower_name)

            # call each process scripts by lower name - TO BE ENHANCED
            if lower_name == "tv_trx_lot":
                log.info("%s job started" % lower_name)
                response = process_tv_trx_lot(log, lower_name, gompb_engine)

                res.append({ "%s" % lower_name: response })

            elif lower_name == "tv_trx_lot_newt":
                log.info("%s job started" % lower_name)
                response = process_tv_trx_lot_newt(log, lower_name, gompb_engine)

                res.append({ "%s" % lower_name: response })
                
            else:
                log.info("%s job pending" % lower_name)
                res.append({ "%s" % lower_name: False })

            print("\n")
            if len(settings.processes) == len(res):
                json_formatted_str = json.dumps(res, indent=2)
                print(json_formatted_str)
            print("\n")
            print("----------------------------------------------")

        end = time()
        elapsed_time = end - start
        print('All processes completed in: {0:3f}s'.format(elapsed_time))

except SQLAlchemyError as e:
    print(e)

    # outdata.update({"epoch": "{0:.3f}s".format(elapsed_time)})
    # outdata.update({"date": "%s" % util_gen.gen_curr_local_date()})

    """
    try:
        # write to json file
        filename_col = "%s_%s.json" % ('log', util_gen.gen_curr_local_date())
        with open('log/%s.txt' % filename_col, 'w') as outfile:
            dump(outdata, outfile)
    except Exception as e:
        log.error(e)
    """

