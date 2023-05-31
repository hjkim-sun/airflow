def divider():
    return     {
        "type": "divider"
    }

def mrkdwn_text(msg):
    return  {
        "type": "mrkdwn",
        "text": f"{msg}"
    }


def section_text(msg):
    return    {
        "type": "section",
        "text": mrkdwn_text(msg)
    }



def fields(lst):
    return   {
        "type": "section",
        "fields": lst
    }