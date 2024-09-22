package com.silzila.provider;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.silzila.exception.BadRequestException;
import com.silzila.exception.RecordNotFoundException;
import com.silzila.payload.request.Query;

import java.sql.SQLException;
import java.util.List;

public interface QueryExecutionStrategy {

    public String getComposedQuery(String userId, String dBConnectionId, String datasetId, List<Query> queries) throws BadRequestException, RecordNotFoundException, SQLException, JsonProcessingException, ClassNotFoundException;

    public String getQueryResult(String userId, String dBConnectionId, String datasetId, List<Query> queries) throws RecordNotFoundException, SQLException, BadRequestException, JsonProcessingException, ClassNotFoundException ;
}
