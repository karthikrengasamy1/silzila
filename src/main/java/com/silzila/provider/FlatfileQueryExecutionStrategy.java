package com.silzila.provider;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.silzila.dto.DatasetDTO;
import com.silzila.exception.BadRequestException;
import com.silzila.exception.RecordNotFoundException;
import com.silzila.helper.RelativeFilterProcessor;
import com.silzila.payload.request.FilterPanel;
import com.silzila.payload.request.Query;
import com.silzila.payload.request.Table;
import com.silzila.querybuilder.QueryComposer;
import com.silzila.service.DatasetService;
import com.silzila.service.DuckDbService;
import com.silzila.service.FileDataService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class FlatfileQueryExecutionStrategy implements QueryExecutionStrategy {

    private static final Logger logger = LogManager.getLogger(FlatfileQueryExecutionStrategy.class);

    @Autowired
    DatasetService datasetService;

    @Autowired
    DuckDbService duckDbService;

    @Autowired
    RelativeFilterProcessor relativeFilterProcessor;

    @Autowired
    FileDataService fileDataService;

    @Autowired
    QueryComposer queryComposer;

    @Override
    public String getComposedQuery(String userId, String dBConnectionId, String datasetId, List<Query> queries) throws RecordNotFoundException, SQLException, BadRequestException, JsonProcessingException, ClassNotFoundException {
        DatasetDTO ds = datasetService.loadDatasetInBuffer(dBConnectionId,datasetId, userId);

        for (Query req : queries) {
            // need at least one dim or measure or field for query execution
            if (req.getDimensions().isEmpty() && req.getMeasures().isEmpty() && req.getFields().isEmpty()) {

                throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                        "Error: At least one Dimension/Measure/Field should be there!");
            }
            // relative Date filter
            // Get the first filter panel from the request
            // Assuming req.getFilterPanels() returns a list of FilterPanel objects
            List<FilterPanel> filterPanels = relativeFilterProcessor.processFilterPanels(req.getFilterPanels(), userId, dBConnectionId, datasetId,datasetService::relativeFilter);
            req.setFilterPanels(filterPanels);
        }

        List<String> tableIds = new ArrayList<String>();
        for (Query req : queries) {
            for (int i = 0; i < req.getDimensions().size(); i++) {
                tableIds.add(req.getDimensions().get(i).getTableId());
            }
            for (int i = 0; i < req.getMeasures().size(); i++) {
                tableIds.add(req.getMeasures().get(i).getTableId());
            }
            for (int i = 0; i < req.getFields().size(); i++) {
                tableIds.add(req.getFields().get(i).getTableId());
            }
            for (int i = 0; i < req.getFilterPanels().size(); i++) {
                for (int j = 0; j < req.getFilterPanels().get(i).getFilters().size(); j++) {
                    tableIds.add(req.getFilterPanels().get(i).getFilters().get(j).getTableId());
                }
            }
        }
        // get distinct table ids
        final List<String> uniqueTableIds = tableIds.stream().distinct().collect(Collectors.toList());
        // get all file Ids (which is inside table obj)
        List<Table> tableObjList = ds.getDataSchema().getTables().stream()
                .filter(table -> uniqueTableIds.contains(table.getId()))
                .collect(Collectors.toList());

        logger.info("unique table id =======\n" + uniqueTableIds.toString() +
                "\ntableObjectList ======== \n" + tableObjList.toString() );
        // throw error when any requested table id is not in dataset
        if (uniqueTableIds.size() != tableObjList.size()) {
            throw new BadRequestException("Error: some table id is not present in Dataset!");
        }

        // get files names from file ids and load the files as Views
        fileDataService.getFileNameFromFileId(userId, tableObjList);
        // come here
        String query = queryComposer.composeQuery(queries, ds, "duckdb");
        logger.info("\n******* QUERY **********\n" + query);
        return query;
    }

    @Override
    public String getQueryResult(String userId, String dBConnectionId, String datasetId, List<Query> queries) throws RecordNotFoundException, SQLException, BadRequestException, JsonProcessingException, ClassNotFoundException {
        String query = getComposedQuery(userId, dBConnectionId, datasetId, queries);
        JSONArray jsonArray = duckDbService.runQuery(query);
        return jsonArray.toString();
    }
}
