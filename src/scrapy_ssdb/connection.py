import six

from scrapy.utils.misc import load_object

from . import defaults


# Shortcut maps 'setting name' -> 'parmater name'.
SETTINGS_PARAMS_MAP = {
    'SSDB_HOST': 'host',
    'SSDB_PORT': 'port',
    'SSDB_ENCODING': 'encoding',
}


def get_ssdb_from_settings(settings):
    """Returns a ssdb client instance from given Scrapy settings object.

    This function uses ``get_client`` to instantiate the client and uses
    ``defaults.SSDB_PARAMS`` global as defaults values for the parameters. You
    can override them using the ``SSDB_PARAMS`` setting.

    Parameters
    ----------
    settings : Settings
        A scrapy settings object. See the supported settings below.

    Returns
    -------
    server
        ssdb client instance.

    Other Parameters
    ----------------
    SSDB_URL : str, optional
        Server connection URL.
    SSDB_HOST : str, optional
        Server host.
    SSDB_PORT : str, optional
        Server port.
    SSDB_ENCODING : str, optional
        Data encoding.
    SSDB_PARAMS : dict, optional
        Additional client parameters.

    """
    params = defaults.SSDB_PARAMS.copy()
    params.update(settings.getdict('SSDB_PARAMS'))
    # XXX: Deprecate SSDB_* settings.
    for source, dest in SETTINGS_PARAMS_MAP.items():
        val = settings.get(source)
        if val:
            params[dest] = val

    # Allow ``ssdb_cls`` to be a path to a class.
    if isinstance(params.get('ssdb_cls'), six.string_types):
        params['ssdb_cls'] = load_object(params['ssdb_cls'])

    return get_ssdb(**params)


# Backwards compatible alias.
from_settings = get_ssdb_from_settings


def get_ssdb(**kwargs):
    """Returns a ssdb client instance.

    Parameters
    ----------
    ssdb_cls : class, optional
        Defaults to ``ssdb3.Client``.
    url : str, optional
        If given, ``ssdb_cls.from_url`` is used to instantiate the class.
    **kwargs
        Extra parameters to be passed to the ``ssdb_cls`` class.

    Returns
    -------
    server
        ssdb client instance.

    """
    ssdb_cls = kwargs.pop('ssdb_cls', defaults.SSDB_CLS)
    return ssdb_cls(**kwargs)
