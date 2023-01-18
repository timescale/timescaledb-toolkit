# Exponential Smoothing

Exponential smoothing is a technique for smoothing time series data wherein exponential functions are used to assign exponentially decreasing weights to observations farther in the past (unlike a simple moving average in which all past observations are weighted equally).


## Simple Exponential Smoothing

The most basic of the exponential smoothing methods is called Simple Exponential Smoothing and is suitable for forecasting data with no clear trend or seasonal pattern.


### Weighted Average Form

Consider the historical time series $y_1, y_2, \ldots, y_T$ where $y_t$ denotes the observation at time $t$.

If we intended to generate forecasts using the average method, in which all future predictions are equal to the average of the observed data, we could write our prediction for the next time period $T + 1$ as $\hat{y}_{T+1|T}$, defined as

$$ \hat{y}_{T+1|T} = \frac{1}{T} y_{T} + \frac{1}{T} y_{T-1} + \ldots + \frac{1}{T} y_{2} + \frac{1}{T} y_{1} $$

Since we would instead like to weight the more recent observations more heavily, we modify our forecast like so:

$$ \hat{y}_{T+1|T} = \alpha y_{T} + \alpha \left(1-\alpha\right) y_{T-1} + \alpha \left(1-\alpha\right)^2 y_{T-2} + \ldots $$

where we introduce a smoothing parameter $0 \leq \alpha \leq 1$ such that the closer $\alpha$ is to 1, the more heavily we rely on recent observations.

We can write the above formulation in a recursive manner to demonstrate that at each time step $t$ simple exponential smoothing is a weighted average between the observation at time $t$ and our previously forecasted value for time $t$.

$$ \hat{y}_{t+1|t} = \alpha y_{t} + \left(1-\alpha\right) \hat{y}_{t|t-1} \qquad t=1,\ldots,T $$

Note we require an initialization parameter, $l_0$, for our base case in the recursion such that we can write:

$$ \hat{y}_{2|1} = \alpha y_{1} + \left(1-\alpha\right) l_{0} $$

This representation does introduce an extra term in the forecasting equation; however, for sufficiently large $T$ its effect is trivial.

$$ \hat{y}_{T+1|T} = \alpha y_{T} + \alpha \left(1-\alpha\right) y_{T-1} + \ldots + \alpha \left(1-\alpha\right)^{T-1} y_{1} + \left(1-\alpha\right)^{T} l_0 $$


### Component Form

We can also write the simple exponential smoothing method as a pair of equations

#### Forecast Equation:  $\hat{y}_{t+1|t} = l_t$  
#### Smoothing (Level) Equation:  $l_t = \alpha y_t + \left(1-\alpha\right) l_{t-1}$  

This abstraction will be more useful when we introduce trend and seasonal components.


### Estimators

Computing the fitted values (and any predicted values) using the simple exponential smoothing method thus requires choosing the value for two parameters, $\alpha$ and $l_0$.

Sometimes, as in the case of the EMA technical indicator discussed in the next section, these values are chosen with a simple heuristic. Though the application of exponential smoothing is usually more reliable when we estimate them using the observed values of the time series.

One statistic used to optimize these estimators is the sum of squared errors (SSE). This is the sum of the squared difference between the observed value $y_t$ and the fitted one $\hat{y}_{t|t-1}$ for all time steps $t$ in our series.

$$ \text{SSE} = \sum_{t=1}^T \left(y_t - \hat{y}_{t|t-1} \right)^2 $$


## Exponential Moving Average

The Exponential Moving Average (EMA) is a technical indicator used in predicting the future price of an asset and is designed to be more sensitive to recent movements in price than the Simple Moving Average.

The Exponential Moving Average, usually specified EMA(N), is actually just simple exponential smoothing applied to the N most recent observations.

However, rather than solve for optimal $\alpha$ and $l_0$, it is typical to set $\alpha = \frac{2}{N+1}$ and $l_0 = y_{T - (N+1)}$.

So we have that the EMA(N) is actually a window function over N+1 observations.


## Double Exponential Smoothing (Holt's Linear Method)

For time series where there is a trend, i.e. a long-term increase or decrease in the data, we can extend the ideas from simple exponential smoothing to account for this behavior in our forecasts.

We can accomplish this by introducing a new component term in our forecast equation. The component form is now characterized by a forecast equation and two smoothing equations.

#### Forecast Equation:  $\hat{y}_{t+1|t} = l_t + b_t$  
#### Level Equation:  $l_t = \alpha y_t + \left(1-\alpha\right) \left(l_{t-1} + b_{t-1}\right)$  
#### Trend Equation:  $b_t = \beta \left(l_t - l_{t-1} \right) + \left(1-\beta\right) b_{t-1}$  

If instead of a one-step ahead forecast, we wanted to forecast $h$ steps into the future, would write the forecast equation as 
#### Forecast Equation:  $\hat{y}_{t+h|t} = l_t + hb_t$

Fittingly, we observe that the forecasts from Holt's Linear Method are a linear function of $h$.


### Damped Trend

Because a constant linear trend isn't always appropriate (might lead to over-estimation of the trend as $h$ increases), we also consider a modification of Holt's linear method that includes a damping parameter $\phi$, $0 \leq \phi \leq 1$.

#### Forecast Equation:  $\hat{y}_{t+h|t} = l_t + \left(\phi + \phi^2 + \ldots + \phi^h \right)b_t$  
#### Level Equation:  $l_t = \alpha y_t + \left(1-\alpha\right) \left(l_{t-1} + \phi b_{t-1}\right)$  
#### Trend Equation:  $b_t = \beta \left(l_t - l_{t-1} \right) + \left(1-\beta\right) \phi b_{t-1}$  

Note that $\phi = 1$ produces the same forecasts as without damping.


### Optimization

We now require fitting smoothing parameters $\alpha$, $\beta$, $\phi$ and initial states $l_0$, $b_0$.


## Triple Exponential Smoothing (Holt-Winters Method)

For time series that demonstrates a seasonal behavior (with period $m$), we can similarly introduce another component term into our equation.

The seasonal component can be either additive or multiplicative in nature. An additive seasonal component is more suited to a time series whose seasonal variations are roughly constant. On the other hand, if the magnitude of seasonal variation is proportional to the level of the series, then multiplicative component is likely more appropriate.

In either case, we now have a forecast equation and three smoothing equations.


### Holt-Winters Additive Method

#### Forecast Equation:  $\hat{y}\_{t+h|t} = l_t + b_t + s\_{t+h-m(k+1)}$  
#### Level Equation:  $l_t = \alpha \left(y_t - s_{t-m}\right) + \left(1-\alpha\right) \left(l_{t-1} + b_{t-1}\right)$  
#### Trend Equation:  $b_t = \beta \left(l_t - l_{t-1} \right) + \left(1-\beta\right) b_{t-1}$  
#### Season Equation:  $s_t = \gamma \left(y_t - l_{t-1} - b_{t-1} \right) + \left(1-\gamma\right) s_{t-m}$  

where $k = \lfloor \frac{h-1}{m} \rfloor$


### Holt-Winters Multiplicative Method

#### Forecast Equation:  $\hat{y}\_{t+h|t} = \left(l\_{t} + hb_{t}\right) s_{t+h-m(k+1)}$  
#### Level Equation:  $l_t = \alpha \frac{y_t}{s_{t-m}} + \left(1-\alpha\right) \left(l_{t-1} + b_{t-1}\right)$  
#### Trend Equation:  $b_t = \beta \left(l_t - l_{t-1} \right) + \left(1-\beta\right) b_{t-1}$  
#### Season Equation:  $s_t = \gamma \frac{y_t}{l_{t-1} - b_{t-1}} + \left(1-\gamma\right) s_{t-m}$  

where $k = \lfloor \frac{h-1}{m} \rfloor$


Note that although both of the Holt-Winters methods used a linear trend component, it is just as possible to combine a damped trend with either an additive or multiplicative seasonal component.


### Optimization

We now require fitting smoothing parameters $\alpha$, $\beta$, $\phi$, $\gamma$ and initial states $l_0$, $b_0$, $s_0,\ldots,s_{-m+1}$.

## ETS Models

We can define the error of our forecasted value at time $t$ as $e_t = y_t - \hat{y}_t$. 

If we assume that these errors are independent and Normally distributed, we have access to statistical models underneath each of the forecasting methods discussed. 
Treating these errors as random variables, we can go beyond forecasting a single value a future to instead giving a prediction interval (at a given confidence level).

...statistical rambling...

Errors can be treated as additive or multiplicative.

In the literature on exponential smoothing it is common therefore to refer to this family of models as ETS models. 
This naming is derived from the model consisting of each an Error, Trend, and Smoothing component.
Additionally ETS can be construed as an initialism for "exponential smoothing".

## Taxonomy of Models


Error component can be
-   Additive (A)
-   Multiplicative (M)

Trend component can be

-   None (N)
-   Additive (A)
-   Additive damped (A<sub>d</sub>)

Seasonal component can be

-   None (N)
-   Additive (A)
-   Multiplicative (M)

For example, Simple Exponential Smoothing is equivalent to an ETS(A,N,N) model and forecasts from an ETS(A,A<sub>d</sub>,M) model are identical to the Holt-Winters Damped Trend Multiplicative Method.

We can usually ignore a couple of possible models because certain combinations of the error, trend, and seasonal components are prone to numerical instability.


## Model Selection

If the user doesn't have a specific model in mind, it is common to fit parameters for each ETS model variant and then use an likelihood measure (e.g. Bayesian Information Criterion) to select the type of model that best represents the data.

