package aeron

import (
	"errors"
	"fmt"

	"github.com/lirm/aeron-go/aeron/command"
)

// A registration the driver rejected reports the driver's error code through *RegistrationError, so a caller can
// tell a retryable code (ErrorCodeResourceTemporarilyUnavailable) from a permanent one.
const regErrMessage = "no room for the channel endpoint"

var regErrCode = int32(command.ErrorCodeResourceTemporarilyUnavailable)

func (c *ClientConductorTestSuite) assertRegistrationError(err error) {
	c.T().Helper()
	c.Require().Error(err)
	var regErr *RegistrationError
	c.Require().True(errors.As(err, &regErr), "want *RegistrationError, got %T", err)
	c.Assert().Equal(int64(CorrelationId1), regErr.RegistrationID)
	c.Assert().Equal(regErrCode, regErr.Code)
	c.Assert().Equal(regErrMessage, regErr.Message)
	c.Assert().Equal(fmt.Sprintf("error on %d: %d: %s", CorrelationId1, regErrCode, regErrMessage), err.Error())
}

func (c *ClientConductorTestSuite) TestPublicationRegistrationErrorCarriesDriverCode() {
	c.driverProxy.On("AddPublication", Channel, StreamId1).Return(CorrelationId1, nil)
	_, err := c.cc.AddPublication(Channel, StreamId1)
	c.Require().NoError(err)
	c.cc.OnErrorResponse(CorrelationId1, regErrCode, regErrMessage)
	pub, err := c.cc.FindPublication(CorrelationId1)
	c.Assert().Nil(pub)
	c.assertRegistrationError(err)
}

func (c *ClientConductorTestSuite) TestSubscriptionRegistrationErrorCarriesDriverCode() {
	c.driverProxy.On("AddSubscription", Channel, StreamId1).Return(CorrelationId1, nil)
	_, err := c.cc.AddSubscription(Channel, StreamId1)
	c.Require().NoError(err)
	c.cc.OnErrorResponse(CorrelationId1, regErrCode, regErrMessage)
	sub, err := c.cc.FindSubscription(CorrelationId1)
	c.Assert().Nil(sub)
	c.assertRegistrationError(err)
}

func (c *ClientConductorTestSuite) TestCounterRegistrationErrorCarriesDriverCode() {
	c.driverProxy.On("AddCounterByLabel", CounterTypeId, CounterLabel).Return(CorrelationId1, nil)
	_, err := c.cc.AddCounterByLabel(CounterTypeId, CounterLabel)
	c.Require().NoError(err)
	c.cc.OnErrorResponse(CorrelationId1, regErrCode, regErrMessage)
	counter, err := c.cc.FindCounter(CorrelationId1)
	c.Assert().Nil(counter)
	c.assertRegistrationError(err)
}
