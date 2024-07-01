try:
    import oracledb
    from utils import util_log
    log = util_log.logger()
except ImportError:
    raise Exception("import util files failed")


def construct_geometry(obj):

    elem_info_extend = []
    ordinates_extend = []

    output_obj = {
        "SDO_GTYPE": 0,
        "SDO_SRID": 0,
        "SDO_POINT": None,
        "SDO_ELEM_INFO": list(),
        "SDO_ORDINATES": list()
    }

    for attr in obj.type.attributes:
        if attr.name == "SDO_ELEM_INFO":
            value = getattr(obj, attr.name)
            if isinstance(value, oracledb.Object):
                for v in value.aslist():
                    elem_info_extend.append(v)

            if len(elem_info_extend) > 0:
               output_obj[attr.name] = elem_info_extend

        elif attr.name == "SDO_ORDINATES":
            value = getattr(obj, attr.name)
            if isinstance(value, oracledb.Object):
                for v in value.aslist():
                    ordinates_extend.append(v)

            if len(ordinates_extend) > 0:
                output_obj[attr.name] = ordinates_extend
        else:
            output_obj[attr.name] = getattr(obj, attr.name)

    # print("output", output_obj)
    return output_obj

def dumpobject(obj, prefix = ""):
    """
    dump object of MDSYS.SDO_GEOMETRY
    """

    if obj.type.iscollection:
        print(prefix, "[")
        for value in obj.aslist():
            if isinstance(value, oracledb.Object):
                dumpobject(value, prefix + "  ")
            else:
                print("value in array", value, type(value))
                print(prefix + "  ", repr(value))
        print(prefix, "]")
    else:
        print(prefix, "{")
        for attr in obj.type.attributes:
            print('attribute', attr.name)
            value = getattr(obj, attr.name)

            print("attr value", value, type(value), isinstance(value, oracledb.Object))

            if isinstance(value, oracledb.Object):
                print(prefix + "   " + attr.name + ":")
                dumpobject(value, prefix + "  ")
            else:
                print(prefix + "   " + attr.name + ":", repr(value))
        print(prefix, "}")

    return True