package com.silzila.provider;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.silzila.dto.DatasetDTO;
import com.silzila.exception.BadRequestException;
import com.silzila.exception.RecordNotFoundException;
import com.silzila.payload.request.Query;
import com.silzila.querybuilder.QueryComposer;
import com.silzila.service.ConnectionPoolService;
import com.silzila.service.DatasetService;
import org.json.JSONArray;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.List;

@Service
public class DatabaseQueryExecutionStrategy implements QueryExecutionStrategy {

    @Autowired
    ConnectionPoolService connectionPoolService;

    @Autowired
    DatasetService datasetService;

    @Autowired
    QueryComposer queryComposer;

    @Override
    public String getComposedQuery(String userId, String dBConnectionId, String datasetId, List<Query> queries) throws BadRequestException, RecordNotFoundException, SQLException, JsonProcessingException, ClassNotFoundException {
        if (dBConnectionId == null || dBConnectionId.isEmpty()) {
            throw new BadRequestException("Error: DB Connection Id can't be empty!");
        }
        String vendorName = connectionPoolService.getVendorNameFromConnectionPool(dBConnectionId, userId);
        DatasetDTO ds = datasetService.loadDatasetInBuffer(dBConnectionId,datasetId, userId);
        String query = queryComposer.composeQuery(queries, ds, vendorName);
        return query;
    }

    @Override
    public String getQueryResult(String userId, String dBConnectionId, String datasetId, List<Query> queries) throws RecordNotFoundException, SQLException, BadRequestException, JsonProcessingException, ClassNotFoundException {
        String query = getComposedQuery(userId, dBConnectionId, datasetId, queries);
        JSONArray jsonArray = connectionPoolService.runQuery(dBConnectionId, userId, query);
        return jsonArray.toString();
    }
}
