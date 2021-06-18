package com.rbkmoney.payout.manager.endpoint;

import com.rbkmoney.payout.manager.PayoutManagementSrv;
import com.rbkmoney.woody.thrift.impl.http.THServiceBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import javax.servlet.*;
import javax.servlet.annotation.WebServlet;
import java.io.IOException;

@Slf4j
@WebServlet("/payout/management")
public class PayoutManagementServlet extends GenericServlet {

    private Servlet thriftServlet;

    @Autowired
    private PayoutManagementSrv.Iface requestHandler;

    @Override
    public void init(ServletConfig config) throws ServletException {
        log.info("Payout management servlet init.");
        super.init(config);
        thriftServlet = new THServiceBuilder()
                .build(PayoutManagementSrv.Iface.class, requestHandler);
    }

    @Override
    public void service(ServletRequest req, ServletResponse res) throws ServletException, IOException {
        log.info("Start new request to servlet.");
        thriftServlet.service(req, res);
    }
}
