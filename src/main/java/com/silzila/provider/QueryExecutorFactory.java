package com.silzila.provider;

import com.silzila.dto.DatasetDTO;
import com.silzila.exception.BadRequestException;

public class QueryExecutorFactory {
    public static QueryExecutionStrategy getExecutionStrategy(DatasetDTO ds) throws BadRequestException {
        if(ds == null){
            throw new BadRequestException("Error: Dataset not found");
        }

        if(ds.getIsFlatFileData() != null){
            return new FlatfileQueryExecutionStrategy();
        }else{
            return new DatabaseQueryExecutionStrategy();
        }

    }
}
