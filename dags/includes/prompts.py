def key_details_command(html):
    command = """
        I need you to extract key details from a HTML document from Redfin for me into json format.
        If there is not a value for an item, the return a blank string.
         
        Here is the HTML document: {input_html}
        
        This is the final json structure expected: 
        {{
            "hoa_fee": "",
            "buyers_agent_fee": ""
        }}
    """.format(input_html=html)

    return command


def listing_agent_command(html):
    command = """
        I need you to extract the listing agents information from a HTML document from Redfin for me into json format. If there is more than one listing agent listed then 
        return data only for the first one. If there is not a value for an item, the return a blank string.
         
        Here is the HTML document: {input_html}
        
        This is the final json structure expected: 
        {{
            "listing_agent_name": "",
            "listing_agent_contact": ""
        }}
    """.format(input_html=html)

    return command


def redfin_agent_command(html):
    command = """
        I need you to extract the Redfin agents information from a HTML document from Redfin for me into json format. If there is more than one redfin agent listed then 
        return data only for the first one. If there is not a value for an item, the return a blank string.
        
        Here is the HTML document: {input_html}
        
        This is the final json structure expected: 
        {{
            "redfin_agent_name":""
        }}
    """.format(input_html=html)

    return command


def public_info_command(html):
    command = """
        I need you to extract the Public Information from a HTML document from Redfin for me into json format.
        If there is not a value for an item, the return a blank string.
         
        Here is the HTML document: {input_html}
        
        This is the final json structure expected: 
        {{
            "finished_sqft":"",
            "unfinished_sqft":"",
            "total_sqft":"",
            "stories":"",
            "style":"",
            "year_built":"",
            "year_renovated":"",
            "county":"",
            "APN":""
        }}
    """.format(input_html=html)

    return command
