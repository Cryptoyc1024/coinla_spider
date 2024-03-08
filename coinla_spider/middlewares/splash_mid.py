# -*- coding: utf-8 -*-


class SplashDownloaderMiddleware(object):
    script = """
    function wait_for_element(splash, css, maxwait)
      if maxwait == nil then
          maxwait = 10
      end
      return splash:wait_for_resume(string.format([[
        function main(splash) {{
          var selector = '%s';
          var maxwait = %s;
          var end = Date.now() + maxwait*1000;
          function check() {{
            if(document.querySelector(selector)) {{
              splash.resume('Element found');
            }} else if(Date.now() >= end) {{
              var err = 'Timeout waiting for element';
              splash.error(err + " " + selector);
            }} else {{
              setTimeout(check, 200);
            }}
          }}
          check();
        }}
      ]], css, maxwait))
    end

    function main(splash, args)
      splash:set_user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
        AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36")
      splash:go("{}")
      wait_for_element(splash, "{}")
      return splash:html()
    end
    """

    def process_request(self, request, spider):
        if 'splash' in request.meta:
            lua_source = self.script.format(request.url, request.meta['css'])
            request.meta['splash']['args']['lua_source'] = lua_source
