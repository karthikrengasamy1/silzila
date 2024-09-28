package com.silzila.provider;

import com.silzila.dto.DatasetDTO;
import com.silzila.exception.BadRequestException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

@Component
public class QueryExecutorFactory {

    @Autowired
    ApplicationContext applicationContext;

    public QueryExecutionStrategy getExecutionStrategy(DatasetDTO ds) throws BadRequestException {
        if(ds == null){
            throw new BadRequestException("Error: Dataset not found");
        }

        if(ds.getIsFlatFileData()){
            return applicationContext.getBean(FlatfileQueryExecutionStrategy.class);
        }else{
            return applicationContext.getBean(DatabaseQueryExecutionStrategy.class);
        }

    }
}
