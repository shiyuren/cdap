/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import React, { Component } from 'react';
import PropTypes from 'prop-types';
import enableSystemApp from 'services/ServiceEnablerUtilities';
import {MyReportsApi} from 'api/reports';
import IconSVG from 'components/IconSVG';
import BtnWithLoading from 'components/BtnWithLoading';
import T from 'i18n-react';
import {isSpark2Available} from 'services/CDAPComponentsVersions';
import Helmet from 'react-helmet';

require('./ReportsServiceControl.scss');

const PREFIX = 'features.Reports.ReportsServiceControl';
const ReportsArtifact = 'cdap-program-report';

export default class ReportsServiceControl extends Component {
  static propTypes = {
    onServiceStart: PropTypes.func
  };

  state = {
    loading: false,
    disabled: false,
    error: null
  };

  componentDidMount() {
    isSpark2Available()
      .subscribe(
        isAvailable => this.setState({
          disabled: !isAvailable
        })
      );
  }

  enableReports = () => {
    this.setState({
      loading: true
    });

    enableSystemApp({
      shouldStopService: false,
      artifactName: ReportsArtifact,
      api: MyReportsApi,
      i18nPrefix: ''
    }).subscribe(
      this.props.onServiceStart,
      () => {
        this.setState({
          error: 'Unable to start Report service',
          loading: false
        });
      }
    );
  };

  renderEnableBtn = () => {
    if (this.state.disabled) {
      return (
        <div className="action-container service-disabled">
          <IconSVG name="icon-exclamation-triangle" className="text-danger" />
          <div className="text-danger">
            Resports require Spark v2. Please upgrade to Spark v2 to enable reports
          </div>
        </div>
      );
    }
    return (
      <div className="action-container">
        <BtnWithLoading
          className="btn-primary"
          label={T.translate(`${PREFIX}.enable`)}
          loading={this.state.loading}
          onClick={this.enableReports}
        />
      </div>
    );
  };

  renderError = () => {
    if (!this.state.error) {
      return null;
    }
    return (
      <div className="reports-service-control-error">
        <h5 className="text-danger">
          <IconSVG name="icon-exclamation-triangle" />
          <span>{T.translate(`${PREFIX}.unableToStart`)}</span>
        </h5>
        <p className="text-danger">
          {this.state.error}
        </p>
      </div>
    );
  }

  render() {
    return (
      <div className="reports-service-control">
        <Helmet title={T.translate('features.Reports.pageTitle')} />
        <div className="image-containers">
          <img className="img-thumbnail" src="/cdap_assets/img/Reports_preview1.png" />
          <img className="img-thumbnail" src="/cdap_assets/img/Reports_preview2.png" />
        </div>
        <div className="text-container">
          <h2> {T.translate(`${PREFIX}.title`)} </h2>
          {this.renderEnableBtn()}
          {this.renderError()}
          <p>
            {T.translate(`${PREFIX}.description`)}
          </p>
          <div className="reports-benefit">
            {T.translate(`${PREFIX}.Benefits.title`)}

            <ul>
              <li>
                <span>{T.translate(`${PREFIX}.Benefits.b1`)}</span>
              </li>
              <li>
                <span>{T.translate(`${PREFIX}.Benefits.b2`)}</span>
              </li>
              <li>
                <span>{T.translate(`${PREFIX}.Benefits.b3`)}</span>
              </li>
              <li>
                <span>{T.translate(`${PREFIX}.Benefits.b4`)}</span>
              </li>
              <li>
                <span>{T.translate(`${PREFIX}.Benefits.b5`)}</span>
              </li>
              <li>
                <span>{T.translate(`${PREFIX}.Benefits.b6`)}</span>
              </li>
            </ul>
          </div>
        </div>
      </div>
    );
  }
}
