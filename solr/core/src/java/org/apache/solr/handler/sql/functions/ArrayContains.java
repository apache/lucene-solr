/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.handler.sql.functions;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.Arrays;
import java.util.List;

public abstract class ArrayContains extends SqlFunction {

    public ArrayContains(String name) {
        super(name, SqlKind.OTHER_FUNCTION, ReturnTypes.BOOLEAN,
                null, null, SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(2);
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding,
                                     boolean throwOnFailure) {
        List<SqlNode> operands = callBinding.getCall().getOperandList();
        SqlNode operand1 = operands.get(0);
        SqlNode operand2 = operands.get(1);
        if (operand1.getKind() == SqlKind.IDENTIFIER) {
            if (operand2.getKind() == SqlKind.LITERAL) {
                return true;
            } else if (operand2.getKind() == SqlKind.ROW) {
                SqlBasicCall valuesCall = (SqlBasicCall) operand2;
                boolean literalMatch =Arrays.stream(valuesCall.getOperands()).allMatch(op -> op.getKind() == SqlKind.LITERAL);
                if (literalMatch) {
                    return true;
                }
            }
        }
        if (throwOnFailure) {
            throw callBinding.newValidationSignatureError();
        } else {
            return false;
        }
    }

    @Override public RelDataType deriveType(SqlValidator validator,
                                            SqlValidatorScope scope, SqlCall call) {
        // To prevent operator rewriting by SqlFunction#deriveType.
        for (SqlNode operand : call.getOperandList()) {
            RelDataType nodeType = validator.deriveType(scope, operand);
            validator.setValidatedNodeType(operand, nodeType);
        }
        return validateOperands(validator, scope, call);
    }
}
